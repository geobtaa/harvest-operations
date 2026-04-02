from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


GRAPHQL_ENDPOINT = "https://api.github.com/graphql"

PROJECT_AND_ISSUE_QUERY = """
query ProjectAndIssue($org: String!, $projectNumber: Int!, $fieldName: String!, $issueId: ID!) {
  organization(login: $org) {
    projectV2(number: $projectNumber) {
      id
      field(name: $fieldName) {
        __typename
        ... on ProjectV2IterationField {
          id
          configuration {
            iterations {
              id
              title
              startDate
              duration
            }
          }
        }
      }
    }
  }
  node(id: $issueId) {
    ... on Issue {
      projectItems(first: 100) {
        nodes {
          id
          project {
            number
            owner {
              __typename
              ... on Organization {
                login
              }
            }
          }
        }
      }
    }
  }
}
"""

ADD_PROJECT_ITEM_MUTATION = """
mutation AddProjectItem($projectId: ID!, $contentId: ID!) {
  addProjectV2ItemById(input: {projectId: $projectId, contentId: $contentId}) {
    item {
      id
    }
  }
}
"""

UPDATE_ITERATION_MUTATION = """
mutation UpdateIteration($projectId: ID!, $itemId: ID!, $fieldId: ID!, $iterationId: String!) {
  updateProjectV2ItemFieldValue(
    input: {
      projectId: $projectId
      itemId: $itemId
      fieldId: $fieldId
      value: {iterationId: $iterationId}
    }
  ) {
    projectV2Item {
      id
    }
  }
}
"""


@dataclass(frozen=True)
class IterationOption:
    id: str
    title: str
    start_date: date
    duration: int


def extract_due_date(issue_body: str) -> date | None:
    match = re.search(r"(?im)^\s*-\s*Due date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*$", issue_body or "")
    if not match:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def due_month_title(due_date: date) -> str:
    return due_date.strftime("%B %Y")


def parse_iteration_options(raw_iterations: list[dict[str, Any]]) -> list[IterationOption]:
    options: list[IterationOption] = []
    for raw_iteration in raw_iterations:
        try:
            options.append(
                IterationOption(
                    id=str(raw_iteration["id"]),
                    title=str(raw_iteration["title"]).strip(),
                    start_date=date.fromisoformat(str(raw_iteration["startDate"])),
                    duration=int(raw_iteration["duration"]),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return options


def find_matching_iteration_id(iterations: list[IterationOption], due_date: date) -> str | None:
    target_title = due_month_title(due_date).lower()
    for iteration in iterations:
        if iteration.title.lower() == target_title:
            return iteration.id

    for iteration in iterations:
        end_date = iteration.start_date + timedelta(days=iteration.duration)
        if iteration.start_date <= due_date < end_date:
            return iteration.id

    return None


def find_project_item_id(issue_data: dict[str, Any], organization: str, project_number: int) -> str | None:
    project_items = (((issue_data.get("projectItems") or {}).get("nodes")) or [])
    for item in project_items:
        project = item.get("project") or {}
        if project.get("number") != project_number:
            continue
        owner = project.get("owner") or {}
        if owner.get("__typename") == "Organization" and owner.get("login") == organization:
            return item.get("id")
    return None


class GitHubGraphQLClient:
    def __init__(self, token: str) -> None:
        self.token = token

    def run(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
        request = Request(
            GRAPHQL_ENDPOINT,
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "Accept": "application/vnd.github+json",
            },
        )

        try:
            with urlopen(request) as response:
                data = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"GitHub API request failed with HTTP {exc.code}: {response_body}") from exc
        except URLError as exc:
            raise RuntimeError(f"GitHub API request failed: {exc.reason}") from exc

        if data.get("errors"):
            messages = "; ".join(error.get("message", "Unknown GraphQL error") for error in data["errors"])
            raise RuntimeError(messages)

        return data.get("data") or {}


def log_annotation(level: str, message: str) -> None:
    print(f"::{level}::{message}")


def ensure_issue_item(
    client: GitHubGraphQLClient,
    project_id: str,
    issue_node_id: str,
    existing_item_id: str | None,
) -> str:
    if existing_item_id:
        return existing_item_id

    data = client.run(
        ADD_PROJECT_ITEM_MUTATION,
        {
            "projectId": project_id,
            "contentId": issue_node_id,
        },
    )
    item_id = (((data.get("addProjectV2ItemById") or {}).get("item")) or {}).get("id")
    if not item_id:
        raise RuntimeError("GitHub did not return a project item ID after adding the issue to the project.")
    return item_id


def sync_issue_iteration() -> int:
    token = os.environ.get("GEOBTAA_PROJECTS_TOKEN", "").strip()
    if not token:
        log_annotation(
            "warning",
            "GEOBTAA_PROJECTS_TOKEN is not configured. Skipping project iteration sync.",
        )
        return 0

    issue_body = os.environ.get("ISSUE_BODY", "")
    issue_node_id = os.environ.get("ISSUE_NODE_ID", "").strip()
    organization = os.environ.get("PROJECT_ORG", "").strip()
    project_number_raw = os.environ.get("PROJECT_NUMBER", "").strip()
    field_name = os.environ.get("PROJECT_ITERATION_FIELD", "Iteration").strip() or "Iteration"

    due_date = extract_due_date(issue_body)
    if due_date is None:
        log_annotation("notice", "No ISO due date found in the issue body. Skipping project iteration sync.")
        return 0

    if not issue_node_id or not organization or not project_number_raw:
        raise RuntimeError("Missing required environment for issue/project lookup.")

    try:
        project_number = int(project_number_raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid PROJECT_NUMBER: {project_number_raw}") from exc

    client = GitHubGraphQLClient(token)
    lookup = client.run(
        PROJECT_AND_ISSUE_QUERY,
        {
            "org": organization,
            "projectNumber": project_number,
            "fieldName": field_name,
            "issueId": issue_node_id,
        },
    )

    organization_data = lookup.get("organization") or {}
    project = organization_data.get("projectV2") or {}
    if not project:
        raise RuntimeError(f"Could not find organization project {organization}/{project_number}.")

    field = project.get("field") or {}
    if field.get("__typename") != "ProjectV2IterationField":
        raise RuntimeError(f"Project field '{field_name}' is missing or is not an iteration field.")

    iterations = parse_iteration_options(((field.get("configuration") or {}).get("iterations")) or [])
    matching_iteration_id = find_matching_iteration_id(iterations, due_date)
    if not matching_iteration_id:
        log_annotation(
            "warning",
            f"No matching iteration found for {due_month_title(due_date)} in {organization}/{project_number}.",
        )
        return 0

    issue = lookup.get("node") or {}
    item_id = ensure_issue_item(
        client,
        str(project["id"]),
        issue_node_id,
        find_project_item_id(issue, organization, project_number),
    )

    client.run(
        UPDATE_ITERATION_MUTATION,
        {
            "projectId": str(project["id"]),
            "itemId": item_id,
            "fieldId": str(field["id"]),
            "iterationId": matching_iteration_id,
        },
    )

    log_annotation(
        "notice",
        f"Set {field_name} to {due_month_title(due_date)} for issue project item {item_id}.",
    )
    return 0


def main() -> int:
    try:
        return sync_issue_iteration()
    except Exception as exc:  # pragma: no cover - surfaced in workflow logs
        log_annotation("error", str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

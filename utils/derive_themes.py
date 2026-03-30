import pandas as pd
import re

def derive_themes_from_keywords(df: pd.DataFrame, theme_map: dict) -> pd.DataFrame:
    """
    Derives a 'Theme' by matching keywords from a provided map against
    the DataFrame's 'Title', 'Keyword', and 'Subject' fields.

    Args:
        df (pd.DataFrame): The DataFrame to process.
        theme_map (dict): A dictionary mapping a lowercase keyword to its corresponding theme.

    Returns:
        pd.DataFrame: The DataFrame with an added 'Theme' column.
    """
    if not theme_map:
        print("[Themes] Theme map is empty or not provided. Skipping theme derivation.")
        return df

    # Pre-compile regex for all keywords for efficiency
    # This creates a single, large regex pattern like: \b(keyword1|keyword2|...)\b
    # It's much faster than iterating and checking one by one in a loop.
    regex_pattern = r'\b(' + '|'.join(re.escape(k) for k in theme_map.keys()) + r')\b'
    keyword_regex = re.compile(regex_pattern, re.IGNORECASE)

    def _find_themes_for_row(row):
        # Combine text from Title, Keyword, and Subject for a comprehensive search
        title_text = str(row.get('Title', ''))
        keyword_text = str(row.get('Keyword', ''))
        subject_text = str(row.get('Subject', ''))
        search_text = ' '.join([title_text, keyword_text, subject_text])
        
        if not search_text.strip():
            return None

        # Find all non-overlapping matches for the keywords in the search text
        found_keywords = set(keyword_regex.findall(search_text.lower()))
        
        if not found_keywords:
            return None

        # Map the found keywords back to their themes using the theme_map
        # Use a set to automatically handle cases where different keywords map to the same theme
        matched_themes = {theme_map[kw] for kw in found_keywords}
        
        if matched_themes:
            # Return a sorted, pipe-separated string of unique themes
            return '|'.join(sorted(list(matched_themes)))
        
        return None

    print("[Themes] Deriving themes from Title, Keyword, and Subject fields...")
    df['Theme'] = df.apply(_find_themes_for_row, axis=1)
    
    return df

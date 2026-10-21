# October 9 ArcGIS parent-pattern audit

This is a read-only audit of the actual primary and distributions CSVs. No live feeds were requested.

- 23,774 dataset records across 152 hubs with dataset rows.
- 94 hubs have sibling layers sharing item IDs: candidates, not confirmed usable parents.
- 1,307 such items contain 7,895 layer records.
- 1 hub has retained parent and child records: HUD-eGIS, with 3 families and 6 child titles that would change.

The filter removed many parents before these CSVs were written. In particular, MnGeo has no retained parent/child pair here despite its known feed pattern. An exact count of affected hubs requires their unfiltered feeds from the same date. Counting sibling layers alone would overstate what has been verified. A one-layer item could also have an omitted parent, so 94 is not an upper bound either.

Source attribution follows the retained record's Code. Shared-ID cleanup may have retained another portal's copy; these figures describe the output, not every source's original catalog.

## Current policy (October 20)

Previously excluded parent records are no longer admitted. The harvester reads their titles before filtering, but the original Shapefile/ImageServer eligibility rule applies to every record. Existing eligible parents remain eligible; existing sublayers retain their IDs. This means the same feed produces the same eligible ID set as before the experimental policy. It does not freeze future harvests to the October 9 records if a source changes its catalog.

At the user's request, parent-title context is now enabled for all Hubs by default. The original distribution eligibility filter is unchanged. The existing MnGeo-specific link mapping remains separate. No schemas, upload logic, or production CSVs were changed. Older experiment CSVs are marked historical rather than overwritten.

23 focused ArcGIS tests pass, including global defaults for multiple Hub codes, exact baseline/updated ID-set equality, hidden-parent title context without admission, retention of already eligible parents, repeat/reordered inputs, and no inferred parent from shared service containers. The full original saved feeds are not currently present in this checkout, so full-feed October 9 demo outputs were not regenerated. Representative raw-feed fixtures remain available for regression tests.

The next full harvest can reveal which sources supply usable catalog parents. The 94 sibling-layer hubs remain an investigation list, not a confirmed count of title changes. MnGeo is confirmed separately by the earlier raw-feed investigation even though its filtered output has no retained parent pairs. No live harvest was run as part of this code change.

## Confirmed retained families

- HUD-eGIS Geospatial Data Storefront: **SAFMR Zip Code Tab Areas** → **SAFMR Zip Code Tab Areas - Small Area Fair Market Rents**
- HUD-eGIS Geospatial Data Storefront: **SAFMR table** → **SAFMR table - Small Area Fair Market Rents**
- HUD-eGIS Geospatial Data Storefront: **COUNTY** → **COUNTY - Tribal Contact Data**
- HUD-eGIS Geospatial Data Storefront: **TRIBE CONTACT COUNTY VW** → **TRIBE CONTACT COUNTY VW - Tribal Contact Data**
- HUD-eGIS Geospatial Data Storefront: **Extensions to Estimated Housing Authority Service Areas Methodology** → **Extensions to Estimated Housing Authority Service Areas Methodology - Layer 0**
- HUD-eGIS Geospatial Data Storefront: **add drop table** → **add drop table - Extensions to Estimated Housing Authority Service Areas Methodology**

## Per-hub findings

Zero visible parent pairs does not mean the feed has none.

| Hub | Code | Dataset rows | Items with sibling layers | Retained parent/child families |
| --- | --- | ---: | ---: | ---: |
| Allegheny County GIS Open Data | 08b-42003 | 50 | 0 | 0 |
| Ashland County, WI GIS Open Data Portal | 10b-55003 | 12 | 0 | 0 |
| Baltimore County Open Data Portal | 04b-24005 | 221 | 0 | 0 |
| Beltrami County Open GIS Data | 05b-27007 | 30 | 0 | 0 |
| Bend Open Data | 17c-04 | 102 | 0 | 0 |
| Benton County Open Data | 16b-55005 | 45 | 1 | 0 |
| Big Stone County Open Data | 05b-27011 | 13 | 2 | 0 |
| Calumet County GIS Download | 10b-55015 | 48 | 1 | 0 |
| Calvert County Open GIS Data Portal | 04b-24009 | 15 | 0 | 0 |
| Capital Area Open Data Portal | 10b-55025 | 1 | 0 | 0 |
| Center for Rural Pennsylvania Data Hub | 8856ec9a-47ac-4c79-bfba-643e97c0b62d | 134 | 24 | 0 |
| Centre County Open Data | 08b-42027 | 90 | 1 | 0 |
| Chester County GIS Data | 08b-42029 | 35 | 4 | 0 |
| Chippewa County Open GIS Data | 05b-27023 | 6 | 1 | 0 |
| City of Bellevue Open Data | 16c-04 | 173 | 6 | 0 |
| City of Champaign GIS | 02c-02 | 33 | 0 | 0 |
| City of Cleveland Open Data | 11c-03 | 171 | 15 | 0 |
| City of Corvallis GIS Data | 17c-05 | 62 | 0 | 0 |
| City of Des Moines GIS Data | 03c-01 | 62 | 0 | 0 |
| City of Eugene Mapping Hub | 17c-02 | 57 | 0 | 0 |
| City of Grand Island Public GIS Data Portal | 13c-02 | 24 | 1 | 0 |
| City of Madison, WI GIS Open Data Portal | 10c-03 | 129 | 0 | 0 |
| City of Newark GIN | 14c-01 | 84 | 7 | 0 |
| City of Saint Paul GIS Data | 05c-02 | 102 | 2 | 0 |
| Clallam County GIS Portal | 16b-53009 | 9 | 0 | 0 |
| Clark County Open Data | 16b-53011 | 58 | 0 | 0 |
| Clay County Open Data | 05b-27027 | 19 | 6 | 0 |
| CMAP (Chicago Metropolitan Agency for Planning) Data Hub | 12f-01 | 325 | 12 | 0 |
| Columbus GIS | 11c-02 | 67 | 0 | 0 |
| Cook Central | 12b-17031 | 394 | 2 | 0 |
| Cook County Open GIS Data | 05b-27031 | 6 | 0 | 0 |
| Crawford County Open Data | 08b-42039 | 71 | 2 | 0 |
| Crow Wing County GIS Hub | 05b-27035 | 12 | 3 | 0 |
| Cuyahoga County Open Data | 11b-39035 | 28 | 4 | 0 |
| Data Salem | 17c-03 | 117 | 8 | 0 |
| Dauphin County GIS Data Portal | 08b-42043 | 24 | 0 | 0 |
| Detroit Open Data | 07c-01 | 271 | 12 | 0 |
| Douglas County Mapping and Data Portal | 16b-53017 | 87 | 5 | 0 |
| Douglas County Open Data Portal | 05b-27041 | 15 | 0 | 0 |
| DuPage County GIS | 12b-17043 | 63 | 5 | 0 |
| Evanston, Illinois Open Data Hub | 02c-01 | 54 | 0 | 0 |
| Evansville, Vanderburgh County, Indiana Open Data Collection | 01b-18163 | 51 | 5 | 0 |
| Franklin County Auditor, Ohio Open Data | 11b-39049 | 31 | 0 | 0 |
| Frederick County Open Data | 04b-24021 | 120 | 2 | 0 |
| GIS Data Downloads for Madison County, Ohio | 11b-39097 | 123 | 12 | 0 |
| GIS Data Downloads for Muskingum County, Ohio | 11b-39119 | 48 | 6 | 0 |
| GIS Divison of Eau Claire County, WI | 10b-55035 | 31 | 4 | 0 |
| Grand Rapids, Michigan Open Data | 07c-03 | 31 | 3 | 0 |
| Grant County Open Data Hub | 05b-27051 | 20 | 1 | 0 |
| Grant County Open Data Portal | 16b-53025 | 59 | 9 | 0 |
| Greene County GIS Hub | 11b-39057 | 43 | 0 | 0 |
| Hamilton County, Indiana Open Data Collection | 01b-18057 | 52 | 1 | 0 |
| Harford County Planning and Zoning Open GIS | 04b-24025 | 60 | 1 | 0 |
| Hennepin County GIS Data | 05b-27053 | 35 | 1 | 0 |
| Holmes County GIS Open Data Portal | 11b-39075 | 17 | 1 | 0 |
| Hubbard County Open Geospatial Data | 05b-27057 | 9 | 0 | 0 |
| HUD-eGIS Geospatial Data Storefront | 1000-0001 | 112 | 3 | 3 |
| Illinois DOT Open Data Portal | 02a-04 | 127 | 5 | 0 |
| IndianaMap | 09a-04 | 695 | 35 | 0 |
| Iowa Geodata | 03a-03 | 156 | 15 | 0 |
| IowaDOT Open Data | 03a-02 | 219 | 33 | 0 |
| Isanti County Geospatial Hub | 05b-27059 | 16 | 0 | 0 |
| Island County Open Data Portal | 16b-53029 | 33 | 1 | 0 |
| Jefferson County Washington Open Data Site | 16b-53031 | 37 | 0 | 0 |
| Kenosha County, WI GIS Open Data Portal | 10b-55059 | 41 | 4 | 0 |
| King County GIS Open Data | 16b-53033 | 517 | 1 | 0 |
| Kittitas County Open Data Portal | 16b-53037 | 25 | 0 | 0 |
| Lake County GIS Hub | 11b-39085 | 13 | 2 | 0 |
| Lake County Illinois Open Data Portal | 12b-17097 | 51 | 1 | 0 |
| Land Records Data Portal of Columbia County, WI | 10b-55021 | 84 | 0 | 0 |
| Lincoln Open Data and Performance Management | 13c-01 | 240 | 8 | 0 |
| Linn County, Iowa Open Data Portal | 03b-19113 | 41 | 0 | 0 |
| Lorain County GIS Data | 11b-39093 | 87 | 3 | 0 |
| Lyon County, MN Mapping Hub | 05b-27083 | 25 | 2 | 0 |
| Maryland iMap | 04a-01 | 1329 | 16 | 0 |
| McLean County GIS | 12b-17113-2 | 1 | 0 | 0 |
| McLean County GIS Consortium Open Data | 02b-17113 | 44 | 1 | 0 |
| Mid-Ohio Open Data | 11f-02 | 37 | 0 | 0 |
| Milwaukee County GIS Download | 10b-55079 | 55 | 2 | 0 |
| Minnesota Geospatial Commons | 05a-01 | 2165 | 233 | 0 |
| Montgomery County GIS Open Data | 04b-24031 | 121 | 2 | 0 |
| Mower County Geospatial Hub | 05b-27099 | 9 | 0 | 0 |
| Nebraska Game & Parks Map and Data Portal | 13a-04 | 28 | 0 | 0 |
| NebraskaMap | 13a-01 | 162 | 9 | 0 |
| New Jersey Department of Environmental Protection's (NJDEP) Bureau of GIS | 14a-02 | 508 | 20 | 0 |
| New Jersey Geographic Information Network (NJGIN) Open Data Portal | 14a-01 | 506 | 40 | 0 |
| North Central PA Hub | 08f-03 | 6 | 0 | 0 |
| Oakland County, MI Open data | 06b-26125 | 151 | 9 | 0 |
| Omaha/Douglas County GIS Open Data Portal | 13b-31055 | 82 | 9 | 0 |
| Open Baltimore | 04c-02 | 483 | 143 | 0 |
| Open Data DC | 04c-01 | 1481 | 2 | 0 |
| Open Data Kitsap County GIS | 16b-53035 | 74 | 0 | 0 |
| Open Data Minneapolis | 05c-01 | 269 | 2 | 0 |
| OpenIndy Data Portal | 01c-02 | 114 | 0 | 0 |
| Opioid Awareness|Peoria City/County Health Department | 12c-02 | 4 | 1 | 0 |
| Oregon GEOHub | 17a-01 | 441 | 14 | 0 |
| Otter Tail County Maps and Data Hub | 05b-27111 | 17 | 1 | 0 |
| Outgamie County Open Data | 10b-55087 | 22 | 0 | 0 |
| Pennsylvania Department of Conservation and Natural Resources Open Data Portal | 08a-02 | 79 | 1 | 0 |
| Pennsylvania Department of Environmental Protection | 08a-03 | 171 | 0 | 0 |
| Peoria County GIS Open Data Portal | 12b-17143 | 76 | 2 | 0 |
| PHLmaps | 08c-01-2 | 617 | 0 | 0 |
| Pickaway County GIS Open Data | 11b-39129 | 167 | 26 | 0 |
| Pierce County GIS Open Data Portal | 10b-55093 | 5 | 0 | 0 |
| Pierce County Open GeoSpatial Data Portal | 16b-53053 | 251 | 0 | 0 |
| Pipestone County Land Records Hub | 05b-27117 | 7 | 1 | 0 |
| Pittsburgh Open Data | 08c-02 | 53 | 0 | 0 |
| Polk County GIS Open Data | 05b-27119 | 56 | 4 | 0 |
| Pope County Open Data | 05b-27121 | 40 | 3 | 0 |
| PortlandMaps Open Data | 17c-06 | 352 | 0 | 0 |
| Pottawattamie County Open Data | 03b-19155 | 235 | 11 | 0 |
| Puget Sound Regional Council Data Portal | 16f-01 | 93 | 0 | 0 |
| Racine County GIS Download | 10b-55101 | 101 | 13 | 0 |
| Ramsey County Open Data | 05b-27123 | 40 | 0 | 0 |
| Renville County Open GIS Data | 05b-27129 | 48 | 10 | 0 |
| RLIS Discovery | 17c-01 | 201 | 1 | 0 |
| Saint Louis County GIS & Mapping | 05b-27137 | 19 | 0 | 0 |
| Sarpy County GIS Portal | 13b-31153 | 94 | 1 | 0 |
| Sauk County GIS Download | 10b-55111 | 31 | 0 | 0 |
| Scott County Open Data | 05b-27139 | 52 | 0 | 0 |
| Seattle GeoData | 16c-01 | 535 | 44 | 0 |
| SEMCOG Open Data | 06f-01 | 55 | 0 | 0 |
| Seneca County GeoLibrary | 11b-39047 | 40 | 3 | 0 |
| Sherburne County Open Data | 05b-27141 | 55 | 3 | 0 |
| Snohomish County Open Data Portal | 16b-53061 | 71 | 1 | 0 |
| South Bend, IN Open Data | 09c-01 | 88 | 1 | 0 |
| Spokane County GIS Data Catalog | 16b-53063 | 42 | 0 | 0 |
| St. Croix County GIS Download | 10b-55109 | 1 | 0 | 0 |
| Stark County GIS Data | 11b-39151 | 99 | 5 | 0 |
| State of Michigan Open Data Portal | 06a-01 | 525 | 30 | 0 |
| State of Washington Geospatial Open Data Portal | 16a-01 | 1156 | 101 | 0 |
| Stearns County GIS Data for Download | 05b-27145 | 1 | 0 | 0 |
| Stevens County Geospatial Hub | 05b-27149 | 5 | 0 | 0 |
| Summit County GIS Data | 11b-39153 | 125 | 10 | 0 |
| Tacoma Open Data | 16c-03 | 284 | 4 | 0 |
| Three Rivers Park District Open Data | 05f-01 | 8 | 2 | 0 |
| Thurston GeoData Center | 16b-53067 | 56 | 1 | 0 |
| Traverse County Open GIS Data | 05b-27155 | 1 | 0 | 0 |
| U.S. Fish and Wildlife Service Open Data | 1000-0003 | 2587 | 202 | 0 |
| UM Matthaei Botanical Gardens and Nichols Arboretum | 07d-02 | 66 | 3 | 0 |
| University of Chicago Library GIS Data Hub | 12d-03 | 33 | 0 | 0 |
| Walla Walla County GIS | 16b-53071 | 55 | 7 | 0 |
| Washington County GIS Download | 10b-55131 | 81 | 9 | 0 |
| Washington County MN Geospatial Maps and Data | 05b-27163 | 7 | 1 | 0 |
| Waukesha County GIS Download | 10b-55133 | 179 | 17 | 0 |
| Waupaca County GIS Download | 10b-55135 | 31 | 0 | 0 |
| Waushara County GIS Download | 10b-55137 | 11 | 0 | 0 |
| Wilkin County, Minnesota Open Data | 05b-27167 | 15 | 0 | 0 |
| Wisconsin Rapids Online Map Portal | 10c-04 | 21 | 1 | 0 |
| Yakima County, WA - Open Data | 16b-53077 | 22 | 2 | 0 |
| Yellow Medicine County Hub | 05b-27173 | 5 | 0 | 0 |
| York County, PA GIS Open Data Portal | 08b-42133 | 49 | 0 | 0 |

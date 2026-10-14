# Source verification — October 14, 2026

The live Seattle Socrata metadata and aggregate queries were fetched during implementation.

- Dataset: `76t5-zqzr`; metadata reports daily refreshes.
- Current row count: 193,613; distinct `permitnum`: 193,613. Keyset pagination uses `permitnum ASC`. Recheck uniqueness on each extraction.
- `permitclassmapped` includes Residential and Non-Residential. The source adapter normalizes these classifications and retains original text.
- Types include Building, Demolition, Roof, Grading, and exemption/exception requests. The dashboard defaults to Building and Demolition. A permit is not a building or a development site.
- `completeddate` exists on 107,741 current rows. The source defines it as the date the permit had all inspections completed. Monthly completion totals require this date. A Completed status alone does not create a completion event.
- `housingunits` is the source-reported net change; `housingunitsadded` and `housingunitsremoved` are component quantities. Missing quantities are retained as unknown. Unit totals are reported permit-level quantities, not independently deduplicated development-site estimates.
- The quarterly ArcGIS layer uses PRMT_NR/RECORDNUMBER and has NEW, DEMO, NET_UNITS and FINAL_DATE fields. It can contain multiple housing-type rows per permit. Optional reconciliation groups these first and only reports coverage/differences. It never changes the main totals. Different coverage and refresh schedules mean disagreements need investigation, not automatic correction.

Endpoints:

- https://data.seattle.gov/api/views/76t5-zqzr.json
- https://data.seattle.gov/resource/76t5-zqzr.json
- https://services.arcgis.com/ZOyb2t4B0UYuYNYH/arcgis/rest/services/Residential_Building_Permits_Issued_and_Final/FeatureServer/0
- https://fred.stlouisfed.org/series/MORTGAGE30US

Full API scans are not transactional snapshots of a live provider. Raw files preserve what was observed during extraction; updates during pagination can create temporary inconsistencies corrected by a later full refresh.

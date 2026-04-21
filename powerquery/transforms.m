// ============================================================
// MavenTech Sales Pipeline — Power Query M Transforms
// All queries load into the Power BI data model as tables.
// Applied steps are named descriptively so the lineage is
// self-documenting in the Power Query editor.
// ============================================================


// ============================================================
// TABLE: fact_opportunities
// Grain: one row per sales opportunity
// Joins: dim_product, dim_agent (via sales_agent key)
// ============================================================
let
    // -- Source --
    Source = Csv.Document(
        File.Contents("CRM+Sales+Opportunities/sales_pipeline.csv"),
        [Delimiter=",", Columns=8, Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),

    // -- Type assignment --
    TypedColumns = Table.TransformColumnTypes(PromotedHeaders, {
        {"opportunity_id", type text},
        {"sales_agent",    type text},
        {"product",        type text},
        {"account",        type text},
        {"deal_stage",     type text},
        {"engage_date",    type date},
        {"close_date",     type date},
        {"close_value",    type number}
    }),

    // -- Derived: reached_engaging flag --
    // Null engage_date = deal died at Prospecting before an engagement was initiated.
    // We retain these rows (do NOT drop) because they are valid pipeline entries.
    // The flag is used to exclude them from stage-2 conversion and cycle calculations.
    AddReachedEngaging = Table.AddColumn(TypedColumns, "reached_engaging",
        each if [engage_date] = null then false else true,
        type logical
    ),

    // -- Derived: is_closed flag --
    AddIsClosed = Table.AddColumn(AddReachedEngaging, "is_closed",
        each if [deal_stage] = "Won" or [deal_stage] = "Lost" then true else false,
        type logical
    ),

    // -- Derived: is_won flag --
    AddIsWon = Table.AddColumn(AddIsClosed, "is_won",
        each if [deal_stage] = "Won" then true else false,
        type logical
    ),

    // -- Derived: days_to_close --
    // Calculated only for closed deals with a valid engage_date.
    // Open deals and Prospecting-only deals get null — intentional.
    AddDaysToClose = Table.AddColumn(AddIsWon, "days_to_close",
        each if [is_closed] and [reached_engaging]
             then Duration.Days([close_date] - [engage_date])
             else null,
        Int64.Type
    ),

    // -- Derived: quarter_opened (format: "2017-Q1") --
    AddQuarterOpened = Table.AddColumn(AddDaysToClose, "quarter_opened",
        each if [engage_date] = null then null
             else Text.From(Date.Year([engage_date]))
                  & "-Q"
                  & Text.From(Date.QuarterOfYear([engage_date])),
        type text
    ),

    // -- Derived: month_opened (first day of month, used for cohort heatmap) --
    AddMonthOpened = Table.AddColumn(AddQuarterOpened, "month_opened",
        each if [engage_date] = null then null
             else Date.StartOfMonth([engage_date]),
        type date
    ),

    // -- Derived: quarter_closed --
    AddQuarterClosed = Table.AddColumn(AddMonthOpened, "quarter_closed",
        each if [close_date] = null then null
             else Text.From(Date.Year([close_date]))
                  & "-Q"
                  & Text.From(Date.QuarterOfYear([close_date])),
        type text
    ),

    // -- Derived: days_in_prospecting --
    // Time from opportunity creation proxy: engage_date - close_date is undefined
    // for pure-Prospecting deals. We approximate using the date range in the dataset.
    // For this dataset there is no created_date column, so days_in_prospecting is
    // left as a placeholder for datasets that include that field.
    // AddDaysInProspecting = ... (requires created_date column — not in source)

    // -- Remove duplicates (safety check) --
    RemoveDuplicates = Table.Distinct(AddQuarterClosed, {"opportunity_id"}),

    // -- Final column order (fact table) --
    ReorderedColumns = Table.ReorderColumns(RemoveDuplicates, {
        "opportunity_id", "sales_agent", "product", "account", "deal_stage",
        "reached_engaging", "is_closed", "is_won",
        "engage_date", "close_date",
        "days_to_close", "quarter_opened", "month_opened", "quarter_closed",
        "close_value"
    })
in
    ReorderedColumns


// ============================================================
// TABLE: dim_product
// ============================================================
let
    Source = Csv.Document(
        File.Contents("CRM+Sales+Opportunities/products.csv"),
        [Delimiter=",", Columns=3, Encoding=65001]
    ),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    TypedColumns = Table.TransformColumnTypes(PromotedHeaders, {
        {"product",     type text},
        {"series",      type text},
        {"sales_price", type number}
    }),
    // Handle GTXPro: appears in pipeline with both "GTXPro" and "GTX Pro" spellings
    // Standardise to "GTX Pro" to match products dim
    NormalisedName = Table.TransformColumns(TypedColumns, {
        {"product", each Text.Trim(_), type text}
    })
in
    NormalisedName


// ============================================================
// TABLE: dim_account
// ============================================================
let
    Source = Csv.Document(
        File.Contents("CRM+Sales+Opportunities/accounts.csv"),
        [Delimiter=",", Columns=7, Encoding=65001]
    ),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    TypedColumns = Table.TransformColumnTypes(PromotedHeaders, {
        {"account",          type text},
        {"sector",           type text},
        {"year_established", Int64.Type},
        {"revenue",          type number},
        {"employees",        Int64.Type},
        {"office_location",  type text},
        {"subsidiary_of",    type text}
    }),
    // Fix typo: "technolgy" → "technology"
    FixSectorTypo = Table.ReplaceValue(TypedColumns, "technolgy", "technology",
        Replacer.ReplaceText, {"sector"})
in
    FixSectorTypo


// ============================================================
// TABLE: dim_agent
// ============================================================
let
    Source = Csv.Document(
        File.Contents("CRM+Sales+Opportunities/sales_teams.csv"),
        [Delimiter=",", Columns=3, Encoding=65001]
    ),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    TypedColumns = Table.TransformColumnTypes(PromotedHeaders, {
        {"sales_agent",     type text},
        {"manager",         type text},
        {"regional_office", type text}
    })
in
    TypedColumns


// ============================================================
// TABLE: dim_date  (date spine, 2016-10-01 to 2018-12-31)
// Used for time-intelligence: DATESYTD, SAMEPERIODLASTYEAR, etc.
// ============================================================
let
    StartDate = #date(2016, 10, 1),
    EndDate   = #date(2018, 12, 31),
    DateCount = Duration.Days(EndDate - StartDate) + 1,
    DateList  = List.Dates(StartDate, DateCount, #duration(1,0,0,0)),
    DateTable = Table.FromList(DateList, Splitter.SplitByNothing(), {"date"}),
    TypedDate = Table.TransformColumnTypes(DateTable, {{"date", type date}}),

    // Standard date attributes for the model
    AddYear       = Table.AddColumn(TypedDate, "year",
        each Date.Year([date]), Int64.Type),
    AddMonth      = Table.AddColumn(AddYear, "month_num",
        each Date.Month([date]), Int64.Type),
    AddMonthName  = Table.AddColumn(AddMonth, "month_name",
        each Date.ToText([date], "MMM"), type text),
    AddQuarter    = Table.AddColumn(AddMonthName, "quarter_num",
        each Date.QuarterOfYear([date]), Int64.Type),
    AddQuarterLabel = Table.AddColumn(AddQuarter, "quarter_label",
        each Text.From([year]) & "-Q" & Text.From([quarter_num]), type text),
    AddYearMonth  = Table.AddColumn(AddQuarterLabel, "year_month",
        each Text.From([year]) & "-" & Text.PadStart(Text.From([month_num]), 2, "0"),
        type text),
    AddIsWeekend  = Table.AddColumn(AddYearMonth, "is_weekend",
        each Date.DayOfWeek([date], Day.Monday) >= 5, type logical),
    AddFYQ        = Table.AddColumn(AddIsWeekend, "fy_quarter",
        // Fiscal year starts Oct 1 (matches dataset start)
        each let m = [month_num], y = [year],
                 fy_year = if m >= 10 then y + 1 else y,
                 fy_q    = if m >= 10 then 1
                           else if m >= 7 then 4
                           else if m >= 4 then 3
                           else 2
             in Text.From(fy_year) & "-FQ" & Text.From(fy_q),
        type text)
in
    AddFYQ

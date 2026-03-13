[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Customer_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Customer = let
  Source = Sql.Database("pragmaticworkspublic.database.windows.net", "AdventureWorksDW"),
  #"Navigation 1" = Source{[Schema = "dbo", Item = "DimCustomer"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 1", {"FactSurveyResponse", "FactInternetSales", "DimGeography", "SpanishOccupation", "FrenchOccupation", "FrenchEducation", "SpanishEducation"}),
  #"Inserted Merged Column" = Table.AddColumn(#"Removed columns", "FullName", each Text.Combine({[FirstName], " ", [LastName]}), type text),
  #"Inserted age" = Table.AddColumn(#"Inserted Merged Column", "Age", each DateTimeZone.LocalNow() - DateTimeZone.From([BirthDate]), type nullable duration),
  #"Inserted total years" = Table.AddColumn(#"Inserted age", "Total years", each Duration.TotalDays([Age]) / 365, type nullable number),
  #"Inserted round down" = Table.AddColumn(#"Inserted total years", "Round down", each Number.RoundDown([Total years]), type nullable Int64.Type),
  #"Removed columns 1" = Table.RemoveColumns(#"Inserted round down", {"Age", "Total years"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns 1", {{"Round down", "Age"}}),
  #"Reordered columns" = Table.ReorderColumns(#"Renamed columns", {"CustomerKey", "GeographyKey", "CustomerAlternateKey", "Title", "FirstName", "MiddleName", "LastName", "FullName", "NameStyle", "BirthDate", "Age", "MaritalStatus", "Suffix", "Gender", "EmailAddress", "YearlyIncome", "TotalChildren", "NumberChildrenAtHome", "EnglishEducation", "EnglishOccupation", "HouseOwnerFlag", "NumberCarsOwned", "AddressLine1", "AddressLine2", "Phone", "DateFirstPurchase", "CommuteDistance"}),
  #"Inserted conditional column" = Table.AddColumn(#"Reordered columns", "Age Breakdown", each if [Age] >= 55 then "55+" else if [Age] >= 45 then "45-54" else if [Age] >= 35 then "35-44" else "18-34"),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted conditional column", {{"Age Breakdown", type text}}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Changed column type", {"CustomerKey", "GeographyKey", "CustomerAlternateKey", "Title", "FirstName", "MiddleName", "LastName", "FullName", "NameStyle", "BirthDate", "Age", "Age Breakdown", "MaritalStatus", "Suffix", "Gender", "EmailAddress", "YearlyIncome", "TotalChildren", "NumberChildrenAtHome", "EnglishEducation", "EnglishOccupation", "HouseOwnerFlag", "NumberCarsOwned", "AddressLine1", "AddressLine2", "Phone", "DateFirstPurchase", "CommuteDistance"})
in
  #"Reordered columns 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Date_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Date = let
  Source = Sql.Database("pragmaticworkspublic.database.windows.net", "AdventureWorksDW"),
  #"Navigation 1" = Source{[Schema = "dbo", Item = "DimDate"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 1", {"SpanishDayNameOfWeek", "FrenchDayNameOfWeek", "SpanishMonthName", "FrenchMonthName", "FactCallCenter", "FactCurrencyRate", "FactFinance", "FactInternetSales(DateKey)", "FactInternetSales(DateKey) 2", "FactInternetSales(DateKey) 3", "FactProductInventory", "FactResellerSales(DateKey)", "FactResellerSales(DateKey) 2", "FactResellerSales(DateKey) 3", "FactSalesQuota", "FactSurveyResponse"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"FullDateAlternateKey", "Date"}})
in
  #"Renamed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Geography_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Geography = let
  Source = Sql.Database("pragmaticworkspublic.database.windows.net", "AdventureWorksDW"),
  #"Navigation 1" = Source{[Schema = "dbo", Item = "DimGeography"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 1", {"SpanishCountryRegionName", "FrenchCountryRegionName", "IpAddressLocator", "DimCustomer", "DimReseller", "DimSalesTerritory", "SalesTerritoryKey"})
in
  #"Removed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Product_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Product = let
  Source = Sql.Database("pragmaticworkspublic.database.windows.net", "AdventureWorksDW"),
  #"Navigation 1" = Source{[Schema = "dbo", Item = "DimProduct"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 1", {"SpanishProductName", "FrenchProductName", "FrenchDescription", "ChineseDescription", "ArabicDescription", "HebrewDescription", "ThaiDescription", "GermanDescription", "JapaneseDescription", "TurkishDescription", "DimProductSubcategory", "FactInternetSales", "FactProductInventory", "FactResellerSales", "LargePhoto"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"EnglishDescription", "Description"}, {"EnglishProductName", "ProductName"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"StartDate", type date}, {"EndDate", type date}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "SalesTerritory_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared SalesTerritory = let
  Source = Sql.Database("pragmaticworkspublic.database.windows.net", "AdventureWorksDW"),
  #"Navigation 1" = Source{[Schema = "dbo", Item = "DimSalesTerritory"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 1", {"SalesTerritoryImage", "DimEmployee", "DimGeography", "FactInternetSales", "FactResellerSales"})
in
  #"Removed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "InternetSales_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared InternetSales = let
  Source = Sql.Database("pragmaticworkspublic.database.windows.net", "AdventureWorksDW"),
  #"Navigation 1" = Source{[Schema = "dbo", Item = "FactInternetSales"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 1", {"DimCurrency", "DimCustomer", "DimDate(DueDateKey)", "DimDate(OrderDateKey)", "DimDate(ShipDateKey)", "DimProduct", "DimPromotion", "DimSalesTerritory", "FactInternetSalesReason"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"ShipDate", type date}, {"DueDate", type date}, {"OrderDate", type date}})
in
  #"Changed column type";
shared Customer_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c5f0b499-f295-4325-92a8-87b7d3800459"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "19895240-7ad6-4925-b3f9-b95549db2e47"]}[Data],
  Navigation_3 = Navigation_2{[Id = "adw", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Customer", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared Date_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c5f0b499-f295-4325-92a8-87b7d3800459"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "19895240-7ad6-4925-b3f9-b95549db2e47"]}[Data],
  Navigation_3 = Navigation_2{[Id = "adw", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Date", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared Geography_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c5f0b499-f295-4325-92a8-87b7d3800459"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "19895240-7ad6-4925-b3f9-b95549db2e47"]}[Data],
  Navigation_3 = Navigation_2{[Id = "adw", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Geography", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared Product_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c5f0b499-f295-4325-92a8-87b7d3800459"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "19895240-7ad6-4925-b3f9-b95549db2e47"]}[Data],
  Navigation_3 = Navigation_2{[Id = "adw", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Product", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared SalesTerritory_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c5f0b499-f295-4325-92a8-87b7d3800459"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "19895240-7ad6-4925-b3f9-b95549db2e47"]}[Data],
  Navigation_3 = Navigation_2{[Id = "adw", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "SalesTerritory", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared InternetSales_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c5f0b499-f295-4325-92a8-87b7d3800459"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "19895240-7ad6-4925-b3f9-b95549db2e47"]}[Data],
  Navigation_3 = Navigation_2{[Id = "adw", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "InternetSales", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

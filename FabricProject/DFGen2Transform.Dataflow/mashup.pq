[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared RawDataAdventureWorks_Product_Categories = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d303dfa9-ba89-4bdc-bb35-30e3a647275b"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "491162e6-65ef-43b3-9976-df40387af5a0"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "RawDataAdventureWorks_Product_Categories"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "AdventureWorks_Product_Categories.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 2, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"ProductCategoryKey", Int64.Type}, {"CategoryName", type text}})
in
  #"Changed column type";
shared RawDataProduct_Subcategories = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d303dfa9-ba89-4bdc-bb35-30e3a647275b"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "491162e6-65ef-43b3-9976-df40387af5a0"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "RawDataProduct_Subcategories"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "AdventureWorks_Product_Subcategories.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 3, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"ProductSubcategoryKey", Int64.Type}, {"SubcategoryName", type text}, {"ProductCategoryKey", Int64.Type}}),
  #"Uppercased text" = Table.TransformColumns(#"Changed column type", {{"SubcategoryName", each Text.Upper(_), type nullable text}})
in
  #"Uppercased text";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Merge_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Merge = let
  Source = Table.NestedJoin(RawDataProduct_Subcategories, {"ProductCategoryKey"}, RawDataAdventureWorks_Product_Categories, {"ProductCategoryKey"}, "RawDataAdventureWorks_Product_Categories", JoinKind.LeftOuter),
  #"Expanded RawDataAdventureWorks_Product_Categories" = Table.ExpandTableColumn(Source, "RawDataAdventureWorks_Product_Categories", {"ProductCategoryKey", "CategoryName"}, {"ProductCategoryKey.1", "CategoryName"}),
  #"Inserted conditional column" = Table.AddColumn(#"Expanded RawDataAdventureWorks_Product_Categories", "CustomFlag", each if [ProductCategoryKey.1] = 1 then "One" else if [ProductSubcategoryKey] = 2 then "Two" else "Three")
in
  #"Inserted conditional column";
shared Merge_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d303dfa9-ba89-4bdc-bb35-30e3a647275b"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "491162e6-65ef-43b3-9976-df40387af5a0"]}[Data],
  TableNavigation = Navigation_2{[Id = "Merge", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

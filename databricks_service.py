# from delta.tables import DeltaTable
# from databricks.connect import DatabricksSession
# from configs.config import get_logger

# logger = get_logger("DatabricksService")

# class DatabricksService:
#     def __init__(self, table_name: str):
#         self.spark = DatabricksSession.builder.getOrCreate()
#         self.table_name = table_name

#     def save_to_table(self, data: list[dict]):
#         try:  
#             df = self.spark.createDataFrame(data)

#             if df.limit(1).count() == 0:
#                 logger.info("==> ℹ️ No Data to upload to Databricks.")
#                 return

#             delta_table = DeltaTable.forName(self.spark, self.table_name)

#             delta_table.alias("target").merge(
#                 df.alias("source"),
#                 "target.box_file_id = source.box_file_id"
#             ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

#             logger.info("==> ✅ Data successfully saved in Databricks.")
#         except Exception as e:
#             logger.exception(f"==> ❌ Error saving data to table: {self.table_name}. Exception: {e}")
#             raise

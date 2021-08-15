import unittest

from desafios.exercicio2.json_schema_to_hive import QueryBuilder


class SchemaToHiveTest(unittest.TestCase):
    query = """
        CREATE EXTERNAL TABLE IF NOT EXISTS
        json_schema_table (
        eid STRING,
		documentNumber STRING,
		name STRING,
		age INT,
		address STRUCT<
		              street STRING,
		              number INT,
		              mailAddress BOOLEAN
		              >
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'      
        """

    def test_correct_query(self):
        QueryBuilder.get_schema("schema.json")
        self.assertEqual(self.query, QueryBuilder.get_create_table_query())

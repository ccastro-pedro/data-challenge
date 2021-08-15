import os

from desafios.exercicio1.event_validator import JsonValidator

_ATHENA_CLIENT = None


class SchemaToTable(JsonValidator):
    schema = None
    cols = None
    default = "s3://bucket/iti-query/"
    dir_path = os.path.dirname(os.path.realpath(__file__))

    @classmethod
    def _get_col_names_and_types(cls, fields):
        cls.cols = fields

    @classmethod
    def _convert_types(cls, typ):
        return {
            'integer': 'INT',
            'string': 'STRING',
            'object': 'STRUCT',
            'array': 'ARRAY',
            'boolean': 'BOOLEAN',
            'null': 'NULL'
        }.get(typ)

    @classmethod
    def _format_columns_type(cls, i, v):
        return f"{i} {cls._convert_types(v.get('type'))}"

    @classmethod
    def get_create_table_query(cls, location=default):
        cls._required_fields_and_types()
        cls._get_col_names_and_types(cls.fields)

        cols_string = ',\n\t\t'.join(cls._format_columns_type(i, v) if v.get('type') != 'object'
                                     else cls._format_columns_type(i, v) +
                                          "<" + f",\n\t\t{' ' * len(cls._format_columns_type(i, v))}"
                                     .join(cls._format_columns_type(j, k) for j, k in v.get("fields", {}).items()) +
                                          f"\n\t\t{' ' * len(cls._format_columns_type(i, v))}>" for i, v in
                                     cls.cols.items())

        query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS
        json_schema_table (
        {cols_string}
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        LOCATION {location}         
        """

        return query


def create_hive_table_with_athena(query):
    """
    Função necessária para criação da tabela HIVE na AWS
    :param query: Script SQL de Create Table (str)
    :return: None
    """

    print(f"Query: {query}")
    ret = _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://iti-query-results/'
        }
    )
    print(ret)


def handler():
    """
    #  Função principal
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função create_hive_table_with_athena para te auxiliar
        na criação da tabela HIVE, não é necessário alterá-la
    """
    SchemaToTable.get_schema('./schema.json')

    query = SchemaToTable.get_create_table_query()

    create_hive_table_with_athena(query)

import os

from desafios.exercicio1.event_validator import JsonValidator

_ATHENA_CLIENT = None


class QueryBuilder(JsonValidator):
    schema = None
    cols = None
    cols_string = None
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
    def _create_cols_string(cls):
        cls._required_fields_and_types()
        cls._get_col_names_and_types(cls.fields)
        cols_string = ''
        for i, v in cls.cols.items():
            if v.get('type') != 'object':
                cols_string += cls._format_columns_type(i, v) + ',\n\t\t'
            else:
                cols_string += cls._format_columns_type(i, v) + "<"
                for j, k in v.get("fields", {}).items():
                    cols_string += f"\n\t\t{' ' * len(cls._format_columns_type(i, v))}" \
                                   + cls._format_columns_type(j, k) + ","
                else:
                    cols_string = cols_string[:-1]
                    cols_string += f"\n\t\t{' ' * len(cls._format_columns_type(i, v))}>,\n\t\t"

        cls.cols_string = cols_string[:len(cols_string) - 4]

    @classmethod
    def get_create_table_query(cls):
        cls._create_cols_string()

        query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS
        json_schema_table (
        {cls.cols_string}
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'      
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
    QueryBuilder.get_schema('./schema.json')

    query = QueryBuilder.get_create_table_query()

    create_hive_table_with_athena(query)

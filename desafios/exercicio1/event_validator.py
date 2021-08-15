import json
import boto3

from utils import get_schema
from utils import get_dict_keys

_SQS_CLIENT = None


class JsonValidator:
    schema = None
    fields = None

    @classmethod
    def get_schema(cls, schema):
        cls.schema = get_schema(schema)


    @classmethod
    def _get_all_fields(cls):
        keys = cls.schema.get('properties').keys()
        print(keys)

    @classmethod
    def _required_fields_and_types(cls, schema=None, fields=None, parent=None):
        # if fields is None:
        #     fields = {}
        # if schema is None:
        #     schema = cls.schema.get('properties')
        #
        # for i, v in schema.items():
        #     if v.get('type') != 'object':
        #         if parent:
        #             fields[parent]['fields'][i] = {'type': v.get('type')}
        #         else:
        #             fields[i] = {'type': v.get('type')}
        #     else:
        #         fields[i] = {'type': v.get('type'), 'fields': {}}
        #         parent = i
        #         return cls._required_fields_and_types(schema.get(i).get('properties'), fields, parent)
        # parent = None
        #         # for j, k in :
        #         #     fields[i] = {'type': v.get('type'), 'fields': {j: {'type': k.get('type')}}}

        cls.fields = {i: ({'type': v.get('type')} if v.get('type') != 'object'
                          else {'type': v.get('type'), 'fields': {j: {'type': k.get('type')} for j, k in cls.schema
                          .get('properties').get(i).get('properties').items()}})
                      for i, v in cls.schema.get('properties').items()}
        # print(fields)
        # cls.fields = fields
        # return cls.fields

    @classmethod
    def _convert_types(cls, typ):
        return {
            'integer': [int],
            'string': [str],
            'object': [dict],
            'array': [list],
            'boolean': [bool],
            'null': [None]
        }.get(typ)

    @classmethod
    def validate(cls, event: dict):
        """
        Faz a validação completa do evento

        :param event: Evento  (dict)
        :return: Bool
        """
        cls._required_fields_and_types()

        # Checa se possui todos os campos necessários:
        if get_dict_keys(event) != get_dict_keys(cls.fields):
            return False

        # Checa o tipo de cada campo:
        try:
            for i, v in event.items():
                if type(v) not in cls._convert_types(cls.fields.get(i).get('type')):
                    return False
                if type(v) == dict and cls.fields.get(i).get('type') == 'object':
                    for j, k in v.items():
                        if type(k) not in cls._convert_types(cls.fields.get(i).get('fields').get(j).get('type')):
                            return False
            return True
        except AttributeError:
            return False


def send_event_to_queue(event, queue_name):
    """
     Responsável pelo envio do evento para uma fila
    :param event: Evento  (dict)
    :param queue_name: Nome da fila (str)
    :return: None
    """

    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response['QueueUrl']
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")


def handler(event):
    """
    #  Função principal que é sensibilizada para cada evento
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função send_event_to_queue para envio do evento para a fila,
        não é necessário alterá-la
    """
    queue_name = 'valid-events-queue'
    JsonValidator.get_schema('./schema.json')
    JsonValidator._get_all_fields()
    if JsonValidator.validate(event):
        send_event_to_queue(event, queue_name)
    else:
        print('Evento inválido')
        return 500, 'Evento inválido'

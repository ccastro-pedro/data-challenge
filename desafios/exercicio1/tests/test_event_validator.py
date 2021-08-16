import unittest

from exercicio1.event_validator import JsonValidator

valid_schema = {
    "eid": "3e628a05-7a4a-4bf3-8770-084c11601a12",
    "documentNumber": "42323235600",
    "name": "Joseph",
    "age": 32,
    "address": {
        "street": "St. Blue",
        "number": 3,
        "mailAddress": True
    }
}

invalid_schema_type = {**valid_schema, **{"age": "32"}}

invalid_schema_extra_field = {**valid_schema, **{"email": "joseph@test.com"}}

invalid_schema_missing_field = {i: v for i, v in valid_schema.items() if i != "documentNumber"}

invalid_schema_type_deeper = {i: (v if i != "address"
                                  else {j: str(k) for j, k in valid_schema.get("address").items()})
                              for i, v in valid_schema.items()}

invalid_schema_extra_field_deeper = {i: (v if i != "address"
                                         else {**valid_schema.get("address"), **{"email": "joseph@test.com"}})
                                     for i, v in valid_schema.items()}

invalid_schema_missing_field_deeper = {i: (v if i != "address"
                                           else {j: k for j, k in valid_schema.get("address").items()
                                                 if j != 'street'}) for i, v in valid_schema.items()}


class EventValidatorTest(unittest.TestCase):
    JsonValidator.get_schema('schema.json')

    def test_valid_configuration(self):
        self.assertTrue(JsonValidator.validate(valid_schema))

    def test_invalid_schema_type(self):
        self.assertFalse(JsonValidator.validate(invalid_schema_type))

    def test_invalid_schema_extra_field(self):
        self.assertFalse(JsonValidator.validate(invalid_schema_extra_field))

    def test_invalid_schema_missing_field(self):
        self.assertFalse(JsonValidator.validate(invalid_schema_missing_field))

    def test_invalid_schema_type_deeper(self):
        self.assertFalse(JsonValidator.validate(invalid_schema_type_deeper))

    def test_invalid_schema_extra_field_deeper(self):
        self.assertFalse(JsonValidator.validate(invalid_schema_extra_field_deeper))

    def test_invalid_schema_missing_field_deeper(self):
        self.assertFalse(JsonValidator.validate(invalid_schema_missing_field_deeper))

from demeter.data import Act, CropType, Field, FieldTrial, Grouper, Organization, Plot

from demeter_utils.query import camel_to_snake

DEMETER_IDS = [
    camel_to_snake(table_level.__name__) + "_id"
    for table_level in [Act, CropType, Field, FieldTrial, Grouper, Plot, Organization]
]

from typing import Any, Dict, Tuple, Set
from .._exceptions import NoUpdateFields, UniqueConstraint
from ..db.database import DBEngine

class update:

    def __init__(self, model=None):
        self._db = DBEngine()
        self._model = model
        self._update_field: Dict[str, Any] = {}
        self._conditions: Tuple[Any, ...] = ()

    def set(self, **kwargs) -> 'update':
        """
            select the model to be fetched
        """
        self._update_field = kwargs
        return self

    def filter(self, *conditions) -> 'update':
        """
            filters the data based on condition
        """
        self._conditions = conditions
        return self

    def __compute_filters(self, data, columns, id_lookup) -> Set[str]:
        temp_data = set()
        for item in data:
            if all([callback(columns, item) for callback in self._conditions]):
                temp_data.add(item[id_lookup])

        return temp_data

    def execute(self):
        table_name = self._model.__classname__
        current_table = self._db.database[table_name]
        id_lookup = current_table.columns[f'{table_name}._id']
        unique_hashes = self._db.database[table_name].unique_hashes
        model_defaults = self._model.__dict__
        filtered_id:Set[str] = set()

        if self._update_field is None or self._update_field == {}:
            raise NoUpdateFields('update  fields are mandatory')

        if self._conditions:
            filtered_id = self.__compute_filters(current_table.data, current_table.columns, id_lookup)

        for idx in range(len(current_table.data)):
            if current_table.data[idx][id_lookup] in filtered_id:

                for fname, value in self._update_field.items():

                    field = model_defaults[fname]
                    fvalue = field.get_defaults(insert_value=value)
                    column_index = current_table.columns[f'{table_name}.{fname}']
                    previous_value = current_table.data[idx][column_index]

                    if field._unique and f'{table_name}.{fname}' in unique_hashes:

                        if previous_value in unique_hashes[f'{table_name}.{fname}']:
                            unique_hashes[f'{table_name}.{fname}'].remove(previous_value)

                        if fvalue in unique_hashes[f'{table_name}.{fname}']:
                            raise UniqueConstraint("values are not unique")

                        unique_hashes[f'{table_name}.{fname}'].add(fvalue)

                    current_table.data[idx][column_index] = fvalue

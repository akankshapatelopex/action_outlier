#!/usr/bin/env python3

import argparse
import inspect
import sys
from pathlib import Path
from urllib.parse import urlparse
import json
from abc import ABCMeta, abstractmethod
from itertools import chain
import operator

# todo add docstrings
def ensure_packages(*package_names):
    import importlib, sys, subprocess
    for package_name in package_names:
        if not importlib.util.find_spec(package_name):
            try:
                subprocess.check_call([
                    sys.executable, '-m', 'pip', 'install', '--extra-index-url',
                    'https://ords:packagesRfun@ords.opexanalytics.com',
                    package_name
                ])
            except subprocess.CalledProcessError as install_error:
                print(
                    f"ERROR: Couldn't install package '{package_name}'",
                    file=sys.stderr
                )
                sys.exit(install_error.returncode)

ensure_packages('ticdat')
from ticdat import TicDatFactory, PanDatFactory


class Action(metaclass=ABCMeta):
    """Every action should inherit from this class."""

    @staticmethod
    def _ensure_docs(action):
        if not inspect.getdoc(action):
            raise Exception(
                f"Add a docstring to '{type(action).__name__}' class"
            )
        for method_name in action.method_names:
            method_doc = inspect.getdoc(getattr(action, method_name))
            if not method_doc:
                raise Exception(
                    f"Add a docstring to '{method_name}' method"
                )
            elif (
                method_name == 'execute_action' and
                inspect.getdoc(getattr(Action, method_name)) == method_doc
            ):
                raise Exception(
                    f"Add a docstring to execute_action method"
                )

    def __new__(action_class, *args, **kwargs):
        action = super().__new__(action_class)
        Action._ensure_docs(action)
        action._data_source_mappings = {'local': {}}
        action.set_local_data_source(
            '',
            (
                Path('../Inputs') if Path('../Inputs').is_dir() else Path('.')
            ).absolute()
        )

        # todo add additional checks like checking for docker.sock
        if len(sys.argv) == 3 and sys.argv[2].endswith('.json'):
            ensure_packages('sqlalchemy')
            from sqlalchemy.engine.url import URL
            from sqlalchemy import create_engine

            scenario_name, config_path = sys.argv[1], sys.argv[2]
            try:
                with open(config_path, 'r') as fp:
                    db_config = json.load(fp)['database']
                    action._enframe_db_url = str(URL(
                        'postgres',
                        username=db_config['dbusername'],
                        password=db_config['dbpassword'],
                        host=db_config['dbserverName'],
                        port=db_config['port'],
                        database=db_config['dbname']
                    ))
            except:
                pass
            else:
                action._is_running_on_enframe = True
                action._enframe_engine = create_engine(
                    action._enframe_db_url
                )
                action._enframe_scenario_name = scenario_name
                action._data_source_mappings['enframe'] = {}
                action.set_enframe_data_source(
                    '', 
                    {
                        'db_url': action.enframe_db_url,
                        'db_schema': action.enframe_scenario_name
                    }
                )
                action.set_enframe_data_source('config_schema', {
                    'db_schema': (
                        type(action).__name__.lower()
                        + '_' + action._enframe_scenario_name
                    )
                })
        return action

    @property
    def is_running_on_enframe(self):
        """Indicates whether the action is running on Enframe or locally"""
        return getattr(self, '_is_running_on_enframe', False)
    
    @is_running_on_enframe.setter
    def is_running_on_enframe(self, value):
        if type(value) is not bool:
            raise ValueError('is_running_on_enframe should be bool')
        setattr(self, '_is_running_on_enframe', value)

    enframe_db_url = property(
        fget=lambda self: getattr(self, '_enframe_db_url', None),
        fset=None, fdel=None,
        doc='URL for Enframe app database of the action'
    )
    
    enframe_scenario_name = property(
        fget=lambda self: getattr(self, '_enframe_scenario_name', None),
        fset=None, fdel=None,
        doc='Name of the Enframe scenario running the action'
    )

    enframe_connection = property(
        fget=lambda self: getattr(self, '_enframe_engine', None),
        fset=None, fdel=None,
        doc='Connection to Enframe app database'
    )

    @property
    def is_setup_on_enframe(self):
        '''Indicates whether the action UI has been setup on Enframe'''
        try:
            schema_names = chain.from_iterable(
                self.enframe_connection.execute(
                    'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA;'
                )
            )
            config_schema_name = (
                type(self).__name__.lower() + '_' + self.enframe_scenario_name
            )
            for schema_name in schema_names:
                if schema_name == config_schema_name:
                    return True
        except:
            pass
        return False

    @staticmethod
    def _is_correct_schema(tic_or_pan_dat, tic_or_pan_dat_schema):
        return (
            (type(tic_or_pan_dat) is
                getattr(tic_or_pan_dat_schema, 'TicDat', None)
            ) or
            (type(tic_or_pan_dat) is
                getattr(tic_or_pan_dat_schema, 'PanDat', None)
            )
        )

    @property
    def config_schema(self):
        '''Configuration schema for the action'''
        return getattr(self, '_config_schema', None)

    @config_schema.setter
    def config_schema(self, tic_or_pan_dat_schema):
        if (
            type(self.config_schema) not in
            (TicDatFactory, PanDatFactory, type(None))
        ):
            raise TypeError(
                "'config_schema' should be a TicDatFactory/PanDatFactory "
                'schema representing the configuration tables for your action'
            )

        self._config_schema = tic_or_pan_dat_schema
        if not tic_or_pan_dat_schema:
            return 

        schema_info = tic_or_pan_dat_schema.schema()
        for table_name, primary_and_data_fields in schema_info.items():
            for field_name in chain.from_iterable(primary_and_data_fields):
                self._config_schema.set_data_type(
                    table_name, field_name, strings_allowed='*',
                    number_allowed=False
                )

    @property
    def config_defaults(self):
        '''Configuration defaults for the action'''
        return getattr(self, '_config_defaults', None)

    @config_defaults.setter
    def config_defaults(self, tic_or_pan_dat):
        if not tic_or_pan_dat:
            self._config_defaults = tic_or_pan_dat
            return

        if not Action._is_correct_schema(tic_or_pan_dat, self.config_schema):
            raise TypeError(
                "'config_defaults' should be a TicDat/PanDat object "
                'containing the default data for your configuration tables.\n'
                'Use self.config_schema.TicDat or self.config_schema.PanDat '
                'depending on whether self.config_schema is a '
                'TicDatFactory/PanDatFactory schema.'
            )
        self._config_defaults = tic_or_pan_dat


    def setup_enframe_ui(self):
        '''Sets up UI for configuration tables on Enframe'''
        if not self.is_running_on_enframe:
            return

        action_name = type(self).__name__
        action_db_name = action_name.lower()
        action_config_display_name = action_name + ' Configuration'

        enframe_ui_tables = {
            'lkp_data_upload_tables': [
                [],
                ['id', 'scenario_template_id', 'order_id', 'tablename',
                 'displayname', 'columnlist', 'displaylist',
                 'columnlistwithtypes', 'visible', 'type', 'unique_key',
                 'visiblecolumns', 'editablecolumns', 'select_query', 'tag',
                 'columnfloat', 'version', 'filter', 'created_at',
                 'updated_at', 'created_by', 'updated_by'
                ]
            ],
            'lkp_views': [
                ['id'], ['table_id', 'definition']
            ],
            'projects': [
                ['id'],
                ['scenario_template_id', 'order_id', 'name', 'tag_id',
                 'status', 'version', 'created_at', 'updated_at', 'archived_at',
                 'created_by', 'updated_by', 'archived_by'
                ]
            ],
            'project_tables': [
                ['id'],
                ['pid', 'name', 'file_name', 'table_name', 'status', 'visible',
                 'type', 'columns', 'created_at', 'updated_at', 'created_by',
                 'updated_by'
                ]
            ]
        }
        self._enframe_ui = TicDatFactory(**enframe_ui_tables)

        self.set_enframe_data_source('_enframe_ui', {'db_schema': 'public'})
        ui_data = self.read_data('_enframe_ui')

        current_project_tuple = next(filter(
            lambda key_and_row: (
                key_and_row[1]['name'].lower().replace(' ', '_') ==
                    self.enframe_scenario_name
            ),
            ui_data.projects.items()
        ), None)
        if not current_project_tuple:
            raise Exception(
                "Couldn't find project associated with current scenario"
            )
        project_id = current_project_tuple[0]
        project_version = current_project_tuple[1]['version']
        scenario_template_id = current_project_tuple[1]['scenario_template_id']

        config_schema_name = action_db_name + '_' +  self.enframe_scenario_name
        self.enframe_connection.execute(
            f'CREATE SCHEMA IF NOT EXISTS {config_schema_name};'
        )
        self.write_data(self.config_defaults, create_tables=True)
        # todo use superticdat for this

        config_schema_info = self.config_schema.schema()
        for config_table_name in self.config_schema.all_tables:
            # table_db_name = action_db_name + '_' + config_table_name
            table_db_name = config_table_name
            table_display_name = table_db_name.replace('_', ' ').title()
            column_display_names = list(
                chain.from_iterable(config_schema_info[table_db_name])
            )
            column_db_names = [
                display_name.lower().replace(' ', '_')
                for display_name in column_display_names
            ]

            column_db_names_str = ','.join(column_db_names)
            column_db_name_and_types_str = ', '.join(
                f'"{column_name}" text'
                for column_name in column_db_names
            )
            column_db_select_str = ', '.join(
                f'"{column_name}" AS "{column_name}"'
                for column_name in column_db_names
            )

            ui_data.project_tables = {
                key: row
                for key, row in ui_data.project_tables.items()
                if row['pid'] != project_id or
                row['table_name'] != table_db_name
            }
            next_project_tables_id = max(chain(ui_data.project_tables, [0])) + 1
            ui_data.project_tables[next_project_tables_id] = {
                'pid': project_id,
                'name': table_display_name,
                'file_name': None,
                'table_name': table_db_name,
                'status': 'Uploaded Successfully',
                'visible': 'true',
                'type': 'input_view',
                'columns': column_db_name_and_types_str,
                'created_at': 'NOW()',
                'updated_at': 'NOW()',
                'created_by': 'Administrator',
                'updated_by': 'Administrator'
            }

            ui_data.lkp_data_upload_tables = [
                row for row in ui_data.lkp_data_upload_tables
                if row['scenario_template_id'] != scenario_template_id or
                row['tablename'] != table_db_name
            ]
            ui_data.lkp_data_upload_tables.append({
                'id': max(
                    chain(
                        (row['id'] for row in ui_data.lkp_data_upload_tables),
                        [0]
                    )
                ) + 1,
                'scenario_template_id': scenario_template_id,
                'order_id': max(
                    chain(
                        (
                            row['order_id']
                            for row in ui_data.lkp_data_upload_tables
                            if row['type'] in ('input', 'input_view')
                        ), [0]
                    )
                ) + 1,
                'tablename': table_db_name,
                'displayname': table_display_name,
                'columnlist': column_db_names_str,
                'displaylist': dict(zip(column_db_names, column_display_names)),
                'columnlistwithtypes': column_db_name_and_types_str,
                'visible': 'true',
                'type': 'input_view',
                'unique_key': '',
                'visiblecolumns': column_db_names_str,
                'editablecolumns': column_db_names_str,
                'select_query': column_db_select_str,
                'tag': action_config_display_name,
                'columnfloat': '{}',
                'version': project_version,
                'filter': '',
                'created_at': 'NOW()',
                'updated_at': 'NOW()',
                'created_by': None,
                'updated_by': None
            })

            self.enframe_connection.execute(
                f'ALTER TABLE {config_schema_name}.{table_db_name} '
                'ADD COLUMN IF NOT EXISTS jqgrid_id INTEGER;'
            )
            self.enframe_connection.execute(
                f'UPDATE {config_schema_name}.{table_db_name} '
                f'SET jqgrid_id = jqgrid.id '
                'FROM (SELECT ROW_NUMBER() OVER () AS id FROM '
                f'{config_schema_name}.{table_db_name}) AS jqgrid'
            )
            self.enframe_connection.execute(
                f'CREATE OR REPLACE VIEW {table_db_name} AS '
                f'SELECT * FROM {config_schema_name}.{table_db_name};'
            )

            # todo change scenario_template_id
            table_id = next(filter(
                lambda row: (
                    row['scenario_template_id'] == scenario_template_id
                    and row['tablename'] == table_db_name
                ),
                ui_data.lkp_data_upload_tables
            ))['id']

            # todo better comparison, i.e. reference the id before it is
            # removed from the lkp_data_upload table
            view_definition = (
                f'SELECT * FROM {config_schema_name}.{table_db_name};'
            )
            ui_data.lkp_views = {
                id: row
                for id, row in ui_data.lkp_views.items()
                if row['definition'] != view_definition
            }
            ui_data.lkp_views[max(chain(ui_data.lkp_views, [0])) + 1] = {
                'table_id': table_id, 
                'definition': view_definition
            }

        for row in ui_data.lkp_data_upload_tables:
            for dict_field in ('displaylist', 'columnfloat'):
                row[dict_field] = json.dumps(row[dict_field])

        # todo use superticdat for writing subschema
        del enframe_ui_tables['projects']
        self._enframe_write_ui = TicDatFactory(**enframe_ui_tables)
        self.set_enframe_data_source(
            '_enframe_write_ui', {'db_schema': 'public'}
        )
        ui_write_data = self._enframe_write_ui.TicDat(
            lkp_data_upload_tables=ui_data.lkp_data_upload_tables,
            # todo see if this is needed
            # project_tables=ui_data.project_tables,
            lkp_views=ui_data.lkp_views
        )
        self.write_data(ui_write_data)

    @staticmethod
    def _get_schema_and_table_name(schema_or_table_name):
        if (
            not isinstance(schema_or_table_name, str)
            or len(schema_or_table_name.split('.')) not in (1, 2)
        ):
            raise ValueError(
                f'Check {schema_or_table_name}\n'
                "'schema_or_table_name' should be a str of the form "
                '<schema_name> or <schema_name>.<table_name>'
            )
        schema_or_table_name_split = schema_or_table_name.split('.')
        if len(schema_or_table_name_split) == 1:
            schema_name, table_name = schema_or_table_name_split[0], None
        else:
            schema_name, table_name = schema_or_table_name_split
            if not table_name:
                raise ValueError(
                    f'Check {schema_or_table_name}\n'
                    '<table_name> in <schema_name>.<table_name> cannot be an '
                    'empty string'
                )
        return schema_name, table_name

    # todo set_hierarchical_params in case of directory
    # todo check this for file_or_dir
    @staticmethod
    def _get_data_source(
        data_source_mappings, schema_or_table_name,
        include_data_source_type=False
    ):
        def get_return_value(data_source_and_type, **kwargs):
            if not include_data_source_type:
                return data_source_and_type[0]
            else:
                return data_source_and_type

        def set_hierarchical_params(
            data_source_and_type, default_data_source_and_type_list
        ):
            def set_default_param(data_source, param):
                if param not in data_source:
                    for (
                        default_data_source, _
                    ) in default_data_source_and_type_list:
                        if param in default_data_source:
                            data_source[param] = default_data_source[param]
                            return

            data_source, data_type = data_source_and_type
            if data_type == 'db':
                set_default_param(data_source, 'db_url')
                set_default_param(data_source, 'db_schema')

        if schema_or_table_name == '' or 'schemas' not in data_source_mappings:
            return get_return_value(data_source_mappings['source'])

        schema_name, table_name = Action._get_schema_and_table_name(
            schema_or_table_name
        )
        if not table_name:
            if schema_name not in data_source_mappings['schemas']:
                data_source_and_type = data_source_mappings['source']
            else:
                data_source_and_type = (
                    data_source_mappings['schemas'][schema_name]['source']
                )
            set_hierarchical_params(
                data_source_and_type, [data_source_mappings['source']]
            )
        else:
            if schema_name not in data_source_mappings['schemas']:
                data_source_and_type = data_source_mappings['source']
            elif (
                table_name not in
                data_source_mappings['schemas'][schema_name]['tables']
            ):
                data_source_and_type = (
                    data_source_mappings['schemas'][schema_name]['source']
                )
            else:
                data_source_and_type = (
                    data_source_mappings['schemas']
                        [schema_name]['tables'][table_name]['source']
                )

            # DB table name is the same as table name in TicDat schema
            # by default. Note that this is not so for schema names
            if data_source_and_type[1] == 'db':
                if 'db_table' not in data_source_and_type[0]:
                    data_source_and_type[0]['db_table'] = table_name
            set_hierarchical_params(
                data_source_and_type, [
                    data_source_mappings['schemas'][schema_name]['source'],
                    data_source_mappings['source']
                ]
            )
        return get_return_value(data_source_and_type)
        
    @staticmethod
    def _get_data_source_type(data_source):
        if isinstance(data_source, str):
            if urlparse(data_source).scheme:
                raise ValueError(
                    f'Check {data_source}\n'
                    "A database 'data_source' should be a dict of the form "
                    '{db_url:..., db_schema:..., db_table:...} with at least '
                    'one of the dict items. All the dict values are strings.\n'
                    "But, if 'db_url' is present then 'db_schema' should also "
                    'be present.'
                )
            return 'file_or_dir'
        elif isinstance(data_source, Path):
            return 'file_or_dir'
        elif isinstance(data_source, dict) and data_source:
            if (
                data_source.get('db_url', None)
                and not data_source.get('db_schema', None)
            ):
                raise ValueError(
                    f'Check {data_source}\n'
                    "If 'db_url' is present then 'db_schema' should also be "
                    "present in the 'data_source' dictionary"
                )
            elif (
                'db_url' in data_source
                and urlparse(data_source['db_url']).scheme not in (
                    'postgresql', 'postgres'
                )
            ):
                raise ValueError(
                    f'Check {data_source}\n'
                    'Only PostgreSQL database is supported at the moment'
                )
            return 'db'
        else:
            raise ValueError(
                f'Check {data_source}\n'
                "'data_source' can represent a file/directory or database.\n"
                "A file/directory 'data_source' can be a str or pathlib.Path "
                'object with the file/directory path.\n'
                "A database 'data_source' should be a dict of the form "
                '{db_url:..., db_schema:..., db_table:...} with at least '
                'one of the dict items. All the dict values are strings.\n'
                "But, if 'db_url' is present then 'db_schema' should also "
                'be present.'
            )

    @staticmethod
    def _check_db_data_source(schema_or_table_name, data_source):
        # db_url without db_schema is handled in _get_data_source_type
        if schema_or_table_name == '':
            if not data_source.get('db_schema', None):
                raise ValueError(
                    f'Check {data_source}\n'
                    "'db_schema' is not present in 'data_source' dict"
                )
            if data_source.get('db_table', None):
                raise ValueError(
                    f'Check {data_source}\n'
                    "'db_table' cannot be specified when setting "
                    'data source for all schemas'
                )

        _, table_name = Action._get_schema_and_table_name(schema_or_table_name)
        if not table_name:
            if not data_source.get('db_schema', None):
                raise ValueError(
                    f'Check {data_source}\n'
                    "'db_schema' is not present in 'data_source' dict"
                )
            if data_source.get('db_table', None):
                raise ValueError(
                    f'Check {data_source}\n'
                    "'db_table' cannot be specified when setting "
                    'data source for a schema'
                )
        else:
            if not data_source.get('db_table', None):
                raise ValueError(
                    f'Check {data_source}\n'
                    "'db_table' is not present in 'data_source' dict"
                )

    # todo data sources can also be ticdat/pandat objects
    @staticmethod
    def _set_data_source(
        data_source_mappings, schema_or_table_name, data_source,
        data_source_type=None
    ):
        data_source_type = (
            data_source_type or Action._get_data_source_type(data_source)
        )
        if data_source_type == 'db':
            Action._check_db_data_source(schema_or_table_name, data_source)

        if schema_or_table_name == '':
            data_source_mappings['source'] = (data_source, data_source_type)
        else:
            schema_name, table_name = Action._get_schema_and_table_name(
                schema_or_table_name
            )
            if not table_name:
                data_source_mappings \
                    .setdefault('schemas', {}) \
                    .setdefault(schema_name, {})['source'] = (
                        data_source, data_source_type
                    )
            else:
                data_source_mappings \
                    .setdefault('schemas', {}) \
                    .setdefault(schema_name, {}) \
                    .setdefault('tables', {}) \
                    .setdefault(table_name, {})['source'] = (
                        data_source, data_source_type
                    )

    def get_enframe_data_source(
        self, schema_or_table_name, include_data_source_type=False
    ):
        '''
        Get the data source being used for a TicDat schema/table when the action
        is running on Enframe
        '''
        return Action._get_data_source(
            self._data_source_mappings['enframe'],
            schema_or_table_name,
            include_data_source_type=include_data_source_type
        )

    def set_enframe_data_source(self, schema_or_table_name, data_source):
        '''
        Set the data source to be used for a TicDat schema/table when the action
        is running on Enframe
        '''
        Action._set_data_source(
            self._data_source_mappings['enframe'],
            schema_or_table_name,
            data_source
        )

    def get_local_data_source(
        self, schema_or_table_name, include_data_source_type=False
    ):
        '''
        Get the data source being used for a TicDat schema/table when the action
        is running locally
        '''
        return Action._get_data_source(
            self._data_source_mappings['local'],
            schema_or_table_name,
            include_data_source_type=include_data_source_type
        )

    def set_local_data_source(self, schema_or_table_name, data_source):
        '''
        Set the data source to be used for a TicDat schema/table when the action
        is running locally
        '''
        Action._set_data_source(
            self._data_source_mappings['local'],
            schema_or_table_name,
            data_source
        )

    @staticmethod
    def _get_data_path_and_type(file_or_dir_path, include_extension=False):
        valid_extensions = [
            'csv', 'json', 'xls', 'xlsx', 'db', 'sql', 'mdb', 'accdb'
        ]
        file_or_dir_path = Path(file_or_dir_path)
        extension = file_or_dir_path.resolve().suffix[1:].lower() or None

        data_path = file_or_dir_path
        if file_or_dir_path.is_file() and extension in valid_extensions:
            data_file_type = extension
            if extension == 'csv':
                data_path = Path(file_or_dir_path).parent
            if extension == 'xlsx':
                data_file_type = 'xls'
            elif extension == 'accdb':
                data_file_type = 'xls'
        elif file_or_dir_path.is_dir():
            # Assumes CSV files are to be read. Reading multiple schemas
            # from different data sources is handled in read_data
            # methods which will then pass the file path rather than
            # directory path if files other than CSV files are to be read
            # NOTE the same doesn't apply to write_data as it will create
            # a file with the schema name if a particular data source isn't
            # specified.
            data_file_type = 'csv'
        else:
            raise TypeError(
                f'Check {file_or_dir_path}\n'
                'TicDat can only read from the following file types '
                f"{valid_extensions}"
            )
        return (
            (data_path, data_file_type) if not include_extension
            else (data_path, data_file_type, extension)
        )

    @staticmethod
    def _read_data_from_file_system(tic_or_pan_dat_schema, file_or_dir_path):
        data_path, data_file_type = Action._get_data_path_and_type(
            file_or_dir_path
        )
        if type(tic_or_pan_dat_schema) is TicDatFactory:
            read_method_name = (
                'create_tic_dat' if data_file_type != 'sql'
                else 'create_tic_dat_from_sql'
            )
        elif type(tic_or_pan_dat_schema) is PanDatFactory:
            read_method_name = 'create_pan_dat'
        return getattr(
            getattr(tic_or_pan_dat_schema, data_file_type),
            read_method_name
        )(data_path)

    @staticmethod
    def _read_data_from_db(
        tic_or_pan_dat_schema, db_engine_or_url, db_schema
    ):
        ensure_packages('sqlalchemy', 'framework_utils')
        from sqlalchemy.engine import Connectable
        from framework_utils.pgtd import PostgresTicFactory
        if isinstance(db_engine_or_url, Connectable):
            db_engine = db_engine_or_url
        else:
            from sqlalchemy import create_engine
            db_engine = create_engine(db_engine_or_url)
        return PostgresTicFactory(tic_or_pan_dat_schema).create_tic_dat(
            db_engine, db_schema
        )

    # todo use superticdat to read subsets of tables
    def read_data(self, *schema_or_table_names):
        '''
        Read data for a TicDat schema/table from its corresponding data source
        '''
        if not all(
            isinstance(schema_or_table_name, str)
            for schema_or_table_name in schema_or_table_names
        ):
            raise ValueError(
                'Every argument should be str of the form <schema_name> '
                'or <schema_name>.<table_name>\n'
                'Here the schema name is the name of the '
                'TicDatFactory/PanDatFactory instance variable bound to the '
                'action object.\n'
                'The table names are those defined in the '
                'TicDatFactory/PanDatFactory object.'
            )

        get_data_source = (
            self.get_enframe_data_source if self.is_running_on_enframe
            else self.get_local_data_source
        )
        data = {}
        for schema_or_table_name in schema_or_table_names:
            schema_name, table_name = Action._get_schema_and_table_name(
                schema_or_table_name
            )
            if table_name:
                raise NotImplementedError(
                    'Reading from individual tables has not been implemented yet'
                )
            tic_or_pan_dat_schema = getattr(self, schema_name, None)
            if not tic_or_pan_dat_schema:
                raise ValueError(
                    f'Check {schema_name}\n'
                    'Cannot find TicDatFactory/PanDatFactory instance variable '
                    'with specified name'
                )

            data_source, data_source_type = get_data_source(
                schema_or_table_name, include_data_source_type=True
            )
            if data_source_type == 'file_or_dir':
                # todo get appropriate file for schema/table
                if Path(data_source).is_dir():
                    pass
                data[schema_or_table_name] = Action._read_data_from_file_system(
                    tic_or_pan_dat_schema, data_source
                )
            elif data_source_type == 'db':
                if (
                    'db_url' not in data_source
                    or 'db_schema' not in data_source
                ):
                    missing_param = (
                        'db_url' if 'db_url' not in data_source
                        else 'db_schema'
                    )
                    raise ValueError(
                        f"The '{missing_param}' in database data source dict "
                        'must be set before reading from a database.\n '
                        'Use set_data_source method to set the db_url '
                        'for any of the following: all schemas, the schema '
                        'to be read or the table to be read.'
                    )
                data[schema_or_table_name] = Action._read_data_from_db(
                    tic_or_pan_dat_schema,
                    (
                        data_source['db_url']
                        if data_source['db_url'] != self.enframe_db_url
                        else self.enframe_connection
                    ),
                    data_source['db_schema']
                )
            setattr(data[schema_or_table_name], '_schema', schema_or_table_name)
        return (
            data if len(schema_or_table_names) > 1
            else next(iter(data.values()))
        )

    def _get_tic_or_pan_dat_schema_name(self, tic_or_pan_dat):
        if hasattr(tic_or_pan_dat, '_schema'):
            return tic_or_pan_dat._schema

        for schema_name in self.schema_names:
            tic_or_pan_dat_schema = getattr(self, schema_name)
            if Action._is_correct_schema(tic_or_pan_dat, tic_or_pan_dat_schema):
                return schema_name
        for attr, value in vars(self).items():
            if (
                type(value) in (TicDatFactory, PanDatFactory)
                and Action._is_correct_schema(tic_or_pan_dat, value)
            ):
                return attr

        raise ValueError(
            f'Check {tic_or_pan_dat}\n'
            "The given TicDat/PanDat doesn't correspond to any "
            'TicDatFactory/PanDatFactory schema defined as an '
            'instance variable/public property of this action'
        )

    def check_data(self, *tic_or_pan_dats):
        '''
        Check if TicDat/PanDat objects violate the data constraints
        defined on their corresponding TicDatFactory/PanDatFactory schemas 
        '''
        for tic_or_pan_dat in tic_or_pan_dats:
            schema_name = self._get_tic_or_pan_dat_schema_name(tic_or_pan_dat)
            tic_or_pan_dat_schema = getattr(self, schema_name)

            assert tic_or_pan_dat_schema.good_pan_dat_object(tic_or_pan_dat)
            # todo see if xls.find_duplicates and so on can be used for
            # ticdatfactory
            if type(tic_or_pan_dat_schema) is PanDatFactory:
                assert not tic_or_pan_dat_schema.find_duplicates(
                    tic_or_pan_dat
                )
            assert not tic_or_pan_dat_schema.find_foreign_key_failures(
                tic_or_pan_dat
            )
            assert not tic_or_pan_dat_schema.find_data_type_failures(
                tic_or_pan_dat
            )
            assert not tic_or_pan_dat_schema.find_data_row_failures(
                tic_or_pan_dat
            )

    @staticmethod
    def _write_data_to_file_system(
        tic_or_pan_dat_schema, tic_or_pan_dat, file_or_dir_path
    ):
        data_path, data_file_type, extension = Action._get_data_path_and_type(
            file_or_dir_path, include_extension=True
        )
        kwargs = {}
        if type(tic_or_pan_dat_schema) is TicDatFactory:
            write_method_name = 'write_file'
            if data_file_type == 'sql':
                write_method_name = (
                    'write_db_data' if extension == 'db'
                    else 'write_sql_file'
                )
            kwargs = {'allow_overwrite': True}
        elif type(tic_or_pan_dat_schema) is PanDatFactory:
            write_method_name = 'write_file'
        if data_file_type == 'csv':
            write_method_name = 'write_directory'

        return getattr(
            getattr(tic_or_pan_dat_schema, data_file_type),
            write_method_name
        )(tic_or_pan_dat, data_path, **kwargs)

    @staticmethod
    def _create_tables_in_db(
        tic_or_pan_dat_schema, db_engine_or_url, db_schema,
    ):
        ensure_packages('sqlalchemy', 'framework_utils')
        from sqlalchemy.engine import Connectable
        from framework_utils.pgtd import PostgresTicFactory
        if isinstance(db_engine_or_url, Connectable):
            db_engine = db_engine_or_url
        else:
            from sqlalchemy import create_engine
            db_engine = create_engine(db_engine_or_url)
        # todo check this
        tic_factory = PostgresTicFactory(tic_or_pan_dat_schema)
        for str in tic_factory._get_schema_sql(tic_or_pan_dat_schema.all_tables, db_schema):
            str = str.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
            db_engine.execute(str)

    @staticmethod
    def _write_data_to_db(
        tic_or_pan_dat_schema, tic_or_pan_dat, db_engine_or_url, db_schema,
        **kwargs
    ):
        ensure_packages('sqlalchemy', 'framework_utils')
        from sqlalchemy.engine import Connectable
        from framework_utils.pgtd import PostgresTicFactory
        if isinstance(db_engine_or_url, Connectable):
            db_engine = db_engine_or_url
        else:
            from sqlalchemy import create_engine
            db_engine = create_engine(db_engine_or_url)
        PostgresTicFactory(tic_or_pan_dat_schema).write_db_data(
            tic_or_pan_dat, db_engine, db_schema,
            **kwargs
        )

    # todo check the pandatfactory test
    # todo add functionality to pass in data types for each field
    def write_data(self, *tic_or_pan_dats, **pgtd_write_db_data_kwargs):
        '''
        Write data for a TicDat schema/table to its corresponding data source
        '''
        if not all(
            (
                'ticdat.ticdatfactory.TicDatFactory.__init__.<locals>.TicDat'
                    in str(type(tic_or_pan_dat))
                or
                'ticdat.pandatfactory.PanDatFactory.__init__.<locals>.PanDat'
                    in str(type(tic_or_pan_dat))
            )
            for tic_or_pan_dat in tic_or_pan_dats
        ):
            raise ValueError(
                'Every argument other than keyword arguments should be a '
                'TicDat/PanDat object'
            )

        get_data_source = (
            self.get_enframe_data_source if self.is_running_on_enframe
            else self.get_local_data_source
        )
        for tic_or_pan_dat in tic_or_pan_dats:
            schema_name = self._get_tic_or_pan_dat_schema_name(tic_or_pan_dat)
            data_source, data_source_type = get_data_source(
                schema_name, include_data_source_type=True
            )
            tic_or_pan_dat_schema = getattr(self, schema_name)

            if data_source_type == 'file_or_dir':
                Action._write_data_to_file_system(
                    tic_or_pan_dat_schema, tic_or_pan_dat, data_source
                )
            elif data_source_type == 'db':
                if (
                    'db_url' not in data_source
                    or 'db_schema' not in data_source
                ):
                    missing_param = (
                        'db_url' if 'db_url' not in data_source
                        else 'db_schema'
                    )
                    raise ValueError(
                        f"The '{missing_param}' in database data source dict "
                        'must be set before writing to a database.\n '
                        'Use set_data_source method to set the db_url '
                        'for any of the following: all schemas, the schema '
                        'to be written or the table to be written.'
                    )
                pgtd_write_db_data_kwargs.setdefault('allow_overwrite', True)
                if pgtd_write_db_data_kwargs.pop('create_tables', False):
                    Action._create_tables_in_db(
                        tic_or_pan_dat_schema,
                        (
                            data_source['db_url']
                            if data_source['db_url'] != self.enframe_db_url
                            else self.enframe_connection
                        ),
                        data_source['db_schema']
                    )
                Action._write_data_to_db(
                    tic_or_pan_dat_schema, tic_or_pan_dat,
                    (
                        data_source['db_url']
                        if data_source['db_url'] != self.enframe_db_url
                        else self.enframe_connection
                    ),
                    data_source['db_schema'],
                    **pgtd_write_db_data_kwargs
                )

    @abstractmethod
    def execute_action(self):
        """
        This method will be called by a user of the action in order
        to execute its functionality. This will be overridden
        by an implementation of the action functionality.
        """

    @property
    def schema_names(self):
        """
        A list of all the public schemas defined by the action.
        Note that these are all the schemas defined as public 
        property attributes of the action.
        """
        # This is required as inspect.getmembers becomes recursive
        # when inspect.getmembers is used on an action object
        if (
            inspect.stack()[1].function == 'getmembers'
            and inspect.stack()[1].filename.endswith('inspect.py')
        ):
            return type(self).schema_names
        else:
            return [
                name
                for name, desc in inspect.getmembers(
                    type(self), inspect.isdatadescriptor
                )
                if not name.startswith('_') and name != 'schema_names'
                and type(desc.fget(self)) in (TicDatFactory, PanDatFactory)
            ]

    @property
    def method_names(self):
        """A list of all the public methods defined by the action"""
        # This is required as inspect.getmembers becomes recursive
        # when inspect.getmembers is used on an action object
        if (
            inspect.stack()[1].function == 'getmembers'
            and inspect.stack()[1].filename.endswith('inspect.py')
        ):
            return type(self).method_names
        else:
            return [
                name
                for name, _ in inspect.getmembers(self, inspect.ismethod)
                if not name.startswith('_')
            ]


def create_template_action():
    raise NotImplementedError()


def create_action_package():
    raise NotImplementedError()


def upload_package():
    raise NotImplementedError()


def main():
    arg_parser = argparse.ArgumentParser(
        description='A package for creating Enframe actions'
    )
    arg_parser.add_argument(
        '--no_template',
        action='store_true',
        default=False,
        help="Don't create a template action"
    )

    arg_parser.add_argument(
        '--create_package',
        action='store_true',
        default=True,
        help='Creates a Python package containing the specified action'
    )
    arg_parser.add_argument(
        '--action_file',
        default=[],
        nargs='*',
        help='Path(s) to file(s) required for the action'
    )
    arg_parser.add_argument(
        '--action_dir',
        default=None,
        nargs='?',
        help='Path to directory containing file(s) required for the action'
    )
    arg_parser.add_argument(
        '--package_dir',
        default='.',
        nargs='?',
        help='Path to the generated action package'
    )

    arg_parser.add_argument(
        '--no_docs',
        action='store_true',
        default=False,
        help=
        "Don't automatically generate documentation from docstrings. "
        'CAUTION: There is no good reason to use this option as the '
        " package generator will not overwrite existing documentation"
    )

    arg_parser.add_argument(
        '--upload_package',
        action='store_true',
        default=False,
        help='Upload the specified action package to OpPy'
    )

    args = arg_parser.parse_args()


if __name__ == '__main__':
    main()
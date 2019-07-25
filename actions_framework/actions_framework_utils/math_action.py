import operator
from itertools import chain

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

# todo package enframe_action
from enframe_action import Action 
ensure_packages('ticdat')
from ticdat import TicDatFactory


class Math(Action):
    """Does math operations on tables"""

    def __init__(self):
        self.config_schema = TicDatFactory(
            data_settings=[
                ['Table Name'], ['Operand 1', 'Operand 2', 'Result']
            ],
            action_settings=[['Parameter'], ['Value', 'Description']]
        )

        self.config_defaults = self.config_schema.TicDat(
            data_settings=[{
                'Table Name': 'data',
                'Operand 1': 'f1',
                'Operand 2': 'f2',
                'Result': 'f3'
            }],
            action_settings=[{
                'Parameter': 'Operator',
                'Value': 'add',
                'Description': 'Operation to perform (add, sub, mul)'
            }]
        )
        
    def execute_action(self):
        """
        Peforms a math operation on tables as specified in the configuration
        """
        config_data = self.read_data('config_schema')
        operator_func = getattr(
            operator,
            config_data.action_settings['Operator']['Value']
        )
        table_config = config_data.data_settings

        self.data_schema = TicDatFactory(**{
            table_name: [
                [],
                [column_names['Operand 1'],
                 column_names['Operand 2'],
                 column_names['Result']
                ]
            ]
            for table_name, column_names in table_config.items()
        })
        data = self.read_data('data_schema')

        for table_name, column_names in table_config.items():
            op1_column_name = column_names['Operand 1']
            op2_column_name = column_names['Operand 2']
            result_column_name = column_names['Result']

            table_data = getattr(data, table_name)
            for data_row in table_data:
                data_row[result_column_name] = (
                    operator_func(
                        float(data_row[op1_column_name]),
                        float(data_row[op2_column_name])
                    )
                )
        self.write_data(data)


if __name__ == '__main__':
    action = Math()
    if action.is_running_on_enframe and not action.is_setup_on_enframe:
        action.setup_enframe_ui()
    else:
        action.execute_action()
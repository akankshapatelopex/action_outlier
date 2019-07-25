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
ensure_packages('pandas', 'ticdat')
import pandas as pd
from ticdat import PanDatFactory


class Math(Action):
    """Does math operations on tables"""

    def __init__(self):
        self.config_schema = PanDatFactory(
            data_settings=[
                [], ['Table Name', 'Operand 1', 'Operand 2', 'Result']
            ],
            action_settings=[['Parameter'], ['Value', 'Description']]
        )

        self.config_defaults = self.config_schema.PanDat(
            data_settings=pd.DataFrame([{
                'Table Name': 'data',
                'Operand 1': 'f1',
                'Operand 2': 'f2',
                'Result': 'f3'
            }]),
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
        config_dfs = self.read_data('config_schema')
        action_config_df = config_dfs.action_settings
        operator_func = getattr(
            operator,
            action_config_df.loc[
                action_config_df['Parameter'] == 'Operator'
            ]['Value'].iloc[0]
        )
        table_config_df = config_dfs.data_settings

        self.data_schema = PanDatFactory(**{
            series['Table Name']: [
                [], [series['Operand 1'], series['Operand 2'], series['Result']]
            ]
            for _, series in table_config_df.iterrows()
        })
        table_dfs = self.read_data('data_schema')

        # sum=0
        for _, table_config_series in table_config_df.iterrows():
            table_name = table_config_series['Table Name']
            op1_column_name = table_config_series['Operand 1']
            op2_column_name = table_config_series['Operand 2']
            result_column_name = table_config_series['Result']

            table_df = getattr(table_dfs, table_name)
            table_df[result_column_name] = table_df.apply(
                lambda row: operator_func(
                    row[op1_column_name], row[op2_column_name]
                ),
                axis=1
            )
            # for item in table_df[op1_column_name]:
            #     sum = sum + float(item)
            #
            # print("Sum is of first operand column is :", sum)

        self.write_data(table_dfs)


if __name__ == '__main__':
    action = Math()
    if action.is_running_on_enframe and not action.is_setup_on_enframe:
        action.setup_enframe_ui()
    else:
        action.execute_action()
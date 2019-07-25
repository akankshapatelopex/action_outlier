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
ensure_packages('pandas', 'ticdat', 'numpy')
import pandas as pd
import numpy as np
from ticdat import PanDatFactory

class Outlier(Action):
    """flags outliers in a column with numerical value (not categorical).
     Class not meant for multi-variate outlier detection;
     Can be extended in future.

    Parameter used for outlier detection: z-score and IQR"""

    def __init__(self):
        self.config_schema = PanDatFactory(
            data_settings=[
                [], ['Table Name', 'Column to analyze', 'Outlier flag column']
            ],
            action_settings=[[], ['Method to use', 'Value (v)', 'Description']]
        )

        self.config_defaults = self.config_schema.PanDat(
            data_settings=pd.DataFrame([{
                'Table Name': 'data',
                'Column to analyze': 'Find outlier',
                'Outlier flag column': 'Result'
            }]),
            action_settings=[{
                'Method to use': 'zScore',
                'Value (v)': '3',
                'Description': 'zScore: the signed number of standard deviations'
                               ' by which the data point is above the mean value'
                               ' of what is being observed or measured. \n '
                               'Outlier = (zScore(data) > v) OR (zScore(data) '
                               '> -v)\n \n'
                               'IQR: Inter-quartile range is the difference ' 
                               'between 75th and 25th percentiles, which are '
                               'Q1 and Q3, respectively.\n'
                               'Outlier = data < (Q1 - v * IQR)) OR ('
                               'data > (Q3 + 1.5 * IQR))'
            }]
        )

    def execute_action(self):
        """
        Peforms outlier/anomaly detection on tables as specified in the
        configuration
        """

        config_dfs = self.read_data('config_schema')

        # parsing action config table into a dataframe
        action_config_df = config_dfs.action_settings

        z_threshold=0
        iqr_multiplier=0
        # TODO This means same method will be used for all row. What if we
        #  need different methods?
        if action_config_df['Method to use'].iloc[0] == 'zScore':
            z_threshold = action_config_df.loc[
                action_config_df['Method to use'] == 'zScore'
                ]['Value (v)'].iloc[0]

        elif action_config_df['Method to use'].iloc[0] == 'IQR':
            iqr_multiplier = action_config_df.loc[
                action_config_df['Method to use'] == 'IQR'
                ]['Value (v)'].iloc[0]
        else:
            print("Error: Please enter a valid value in Method to use.")

        print("Parsed action config:, z_threshold: ", z_threshold,
              ", IQR multiplier: ", iqr_multiplier)
        # reading data config table
        table_config_df = config_dfs.data_settings

        self.data_schema = PanDatFactory(**{
            series['Table Name']: [
                [], [series['Column to analyze'], series['Outlier flag column']]
            ]
            for _, series in table_config_df.iterrows()
        })
        table_dfs = self.read_data('data_schema')

        # processing data
        for _, table_config_series in table_config_df.iterrows():
            table_name = table_config_series['Table Name']
            col_to_analyze = table_config_series['Column to analyze']
            col_outlier = table_config_series['Outlier flag column']
            table_df = getattr(table_dfs, table_name)

            numbers_l = []
            for item in table_df[col_to_analyze]:
                numbers_l.append(float(item))

            numpy_num_array = np.array(numbers_l)
            std_dev = np.std(numpy_num_array)
            mean = np.mean(numbers_l)

            print("Mean: ", mean, "Standard deviation: ", std_dev)

            if action_config_df['Method to use'].iloc[0] == 'zScore':
                table_df[col_outlier] = table_df.apply(
                    lambda row: 1 if (row[col_to_analyze]-mean)/std_dev
                                     > z_threshold else 0
                    ,
                    axis=1
                )
            else:
                print("You have chosen IRQ")

        #writing data
        self.write_data(table_dfs)


if __name__ == '__main__':
    action = Outlier()
    if action.is_running_on_enframe and not action.is_setup_on_enframe:
        action.setup_enframe_ui()
    else:
        action.execute_action()
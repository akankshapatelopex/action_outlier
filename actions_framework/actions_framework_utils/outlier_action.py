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
            action_settings=[[], ['Table', 'Column', 'Method', 'Value',
                                  'Flag Column']]
        )

        self.config_defaults = self.config_schema.PanDat(
            action_settings=[{
                'Table': 'data',
                'Column': 'column1',
                'Method': 'zScore',
                'Value': '3',
                'Flag Column': 'flag'
            }]
        )

    def execute_action(self):
        """
        Performs outlier/anomaly detection on fields/columns based on zScore,
        inter-quartile range or bounds
        """

        config_dfs = self.read_data('config_schema')

        # parsing action config table into a dataframe
        action_config_df = config_dfs.action_settings

        table_col_dict = {}
        table_flag_col_dict = {}
        for _, series in action_config_df.iterrows():
            if series['Table'] in table_col_dict.keys():
                table_col_dict[series['Table']].add(str(series['Column']))
            else:
                table_col_dict[series['Table']] = {series['Column']}
                table_flag_col_dict[series['Table']] = {series['Flag '
                                                                  'Column']}
            # print("table_col_dict", table_col_dict)
            # print("table_flag_col_dict", table_flag_col_dict)

        self.data_schema = PanDatFactory(**{
            table: [
                [], list(table_col_dict[table]) + list(table_flag_col_dict[
                                                           table])
            ]
            for table in table_col_dict.keys()
        })

        table_dfs = self.read_data('data_schema')

        # processing data in dataframe

        for i in range(0,action_config_df.shape[0]):
            # print("\n *** Executing row", i, "in actions ***")
            table_name=action_config_df['Table'].iloc[i]
            col_to_analyze=action_config_df['Column'].iloc[i]
            flag_column=action_config_df['Flag Column'].iloc[i]
            # print("Table: ", table_name, ", column to analyze: ",
            #       col_to_analyze, ", flag column: ", flag_column)
            z_threshold = 0
            iqr_multiplier = 0
            lb = 0
            ub = 0
            if action_config_df['Method'].iloc[i] == 'zScore':
                z_threshold = float(action_config_df['Value'].iloc[i])
                table_df = getattr(table_dfs, table_name)

                numbers_l = []
                for item in table_df[col_to_analyze]:
                    numbers_l.append(float(item))
                numpy_num_array = np.array(numbers_l)
                std_dev = np.std(numpy_num_array)
                mean = np.mean(numbers_l)
                # print("Mean: ", mean, "Standard deviation: ", std_dev)
                # print("z_threshold: ", z_threshold)

                table_df[flag_column] = table_df.apply(
                    lambda row: bool(row[flag_column]) or
                                (True
                                 if (((float(row[col_to_analyze]) - mean) /
                                            std_dev > z_threshold)
                                        or
                                     ((float(row[col_to_analyze]) - mean) /
                                            std_dev < -z_threshold))
                                else False)
                    , axis=1)


            elif action_config_df['Method'].iloc[i] == 'IQR':
                iqr_multiplier = float(action_config_df['Value'].iloc[i])
                table_df = getattr(table_dfs, table_name)

                numbers_l = []
                for item in table_df[col_to_analyze]:
                    numbers_l.append(float(item))
                numpy_num_array = np.array(numbers_l)
                q75, q25 = np.percentile(numpy_num_array, [75, 25])
                iqr = q75 - q25
                # print("First percentile: ", q25, "Third percentile: ", q75,
                #       "Inter quartile range: ", iqr)
                # print("IQR multiplier: ", iqr_multiplier)

                table_df[flag_column] = table_df.apply(
                    lambda row: bool(row[flag_column]) or
                                (True if ((float(row[col_to_analyze]) >
                                           q75 + (iqr_multiplier * iqr))
                                        or
                                        (float(row[col_to_analyze]) < q25
                                         - (iqr_multiplier * iqr))
                                        )
                                else False)
                    , axis=1)

            elif action_config_df['Method'].iloc[i] == 'range':
                lb, ub = action_config_df['Value'].iloc[i].split(',')
                # print("Lower bound = ", lb,
                #       ", Upper bound =", ub)
                table_df = getattr(table_dfs, table_name)

                table_df[flag_column] = table_df.apply(
                    lambda row: bool(row[flag_column]) or
                                (True if ((float(row[col_to_analyze]) >
                                           float(ub))
                                          or
                                          (float(row[col_to_analyze]) <
                                           float(lb)))
                                 else False)
                    , axis=1)

            else:
                print("Error: Please enter a valid value in Method to use ("
                      "zScore, IQR, range).")




        # writing data
        self.write_data(table_dfs)

        exit()


if __name__ == '__main__':
    action = Outlier()
    if action.is_running_on_enframe and not action.is_setup_on_enframe:
        action.setup_enframe_ui()
    else:
        action.execute_action()
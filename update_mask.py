"""
    author: Terry Lee
    created: 2022-03-03
    updated: 2022-03-18 - fillna('') added for both dataframes otherwise NaN will always return False(different) when compared

    Description:
        Compare two dataframes to see if they are same, highlight the different cell values between the two.
        In order to compare, below conditions should be met.
            Columns names and count should be same
            Data size should be same
            Date type columns should be in string type -> astype('str) -> 
                bug found when changing date to str, 00:00:00 is added for time leading to comparing two dates 
                one with time and one without
            NaN should not exist -> fillna('')
            Index should be same -> reset_index
"""

import pandas as pd

class UpdateMask():
    def __init__(self, df_before, df_after, col_list, file_path_mask, col_dup_check):
        self.df_before = df_before.copy()
        self.df_after = df_after.copy()
        UpdateMask.change_all_coltypes_to_str(self.df_before)
        UpdateMask.change_all_coltypes_to_str(self.df_after)
        self.df_before.fillna('')
        self.df_after.fillna('')
        self.col_dup_check = col_dup_check

        # 1. reset index on both df
        self.df_before.reset_index(inplace = True, drop = True)
        self.df_after.reset_index(inplace = True, drop = True)

        self.col_list = col_list
        self.file_path_mask = file_path_mask
        
        # 2. loop through bigger df and add to df at the last pos (with all null values except for designated column - for key id)
        # self.perform_comparison()
        self.df_after, self.df_before = UpdateMask.make_two_dataframes_identically_labelled(self.df_after, self.df_before, self.col_list)

        # 3. if df_after is small then re-adjust the mask
        self.save_mask()

    # def perform_comparison(self):
    #     self.df_after, self.df_before = UpdateMask.make_two_dataframes_identically_labelled(self.df_after, self.df_before, self.col_list)

    def readjust_mask_with_new_size(self):
        print('readjust mask.. ')
        df_tmp = self.df_after[self.df_after[self.col_dup_check].isnull() == True]
        if len(df_tmp) > 0:
            print('empty row found')
            index = df_tmp.index[0]
            self.update_mask = self.update_mask.drop(index)
            print('Index to delete: {}'.format(index))
        else:
            print('nothing to delete')

    def save_mask(self):
        self.update_mask = self.df_after == self.df_before
        self.readjust_mask_with_new_size()
        self.update_mask.to_csv(self.file_path_mask, index = False)
        self.df_before.to_csv(r'latest_before.csv', index = False)

    @staticmethod
    def make_two_dataframes_identically_labelled(df1, df2, cols_to_compare):
        # need to loop through the bigger dataframes to cover all the items to add
        if len(df1) >= len(df2):
            df1, df2 = UpdateMask._make_two_dataframes_identically_labelled(df1, df2, cols_to_compare)
        else:
            df2, df1 = UpdateMask._make_two_dataframes_identically_labelled(df2, df1, cols_to_compare)

        return df1, df2 
    
    @staticmethod
    def _make_two_dataframes_identically_labelled(df1, df2, cols_to_compare):

        df1 = df1.sort_values(by = cols_to_compare).reset_index(drop = True)
        df2 = df2.sort_values(by = cols_to_compare).reset_index(drop = True)
        
        print('size of df - df1: {}, df2: {}'.format(len(df1), len(df2)))
        for index, _ in df1.iterrows():
            # print('index {}'.format(index))

            if index >= len(df2):
                df2 = df2.append(pd.Series(), ignore_index = True)
                print('row is added to small df')
            else:
                are_same = True
                cur_row_value1 = {}
                cur_row_value2 = {}
                for col in cols_to_compare:
                    cur_row_value1[col] = df1.loc[index, col]
                    cur_row_value2[col] = df2.loc[index, col]
                    
                    if df1.loc[index, col] != df2.loc[index, col]:
                        print('{} <> {}'.format(df1.loc[index, col], df2.loc[index, col]))
                        are_same &= False

                if not are_same:
                    print('not same at index: {}'.format(index))
                    line = pd.DataFrame([], columns = df1.columns)

                    df_tmp = UpdateMask.values_found_in_df(cur_row_value1, df2.loc[index + 1:])
                    if len(df_tmp) == 0:
                        print('adding in df2: {} ..'.format(cur_row_value1[cols_to_compare[0]]))
                        for col in cols_to_compare:
                            line.loc[0, col] = df1.loc[index, col]
                        
                        df2 = pd.concat([df2.iloc[:index], line, df2.iloc[index:]]).reset_index(drop=True)
                    else:
                        print('adding in df1: {} ..'.format(cur_row_value2[cols_to_compare[0]]))
                        for col in cols_to_compare:
                            line.loc[0, col] = df2.loc[index, col]

                        df1 = pd.concat([df1.iloc[:index], line, df1.iloc[index:]]).reset_index(drop=True)
                else:
                    # print('same at index: {}'.format(index))
                    pass

        df1 = df1.sort_values(by = cols_to_compare)
        df2 = df2.sort_values(by = cols_to_compare)
        
        if len(df1) != len(df2):
            print('FAILED (_make_two_dataframes_identically_labelled): df size different! df1: {}, df2: {}'.format(len(df1), len(df2)))
        return df1, df2

    @staticmethod
    def convert_df_to_list_of_str(df_tgt):
        lines = []
        for index, row in df_tgt.iterrows():
            line = ''
            for col in df_tgt.columns:
                line += (str(row[col]) + '|')
            line = line.strip('|')
            lines.append(line)    
        return lines

    @staticmethod
    def values_found_in_df(dict_values, df_tgt):
        mask = True

        res = pd.DataFrame(columns=df_tgt.columns)
        try:
            for col in dict_values:
    #             print('col: {}, dict_values[col]: {}'.format(col, dict_values[col]))
                mask = mask & (df_tgt[col] == dict_values[col])

            res = df_tgt[mask]
        except KeyError:
            print('Exception occurred: KeyError')
        return res

    @staticmethod
    def change_all_coltypes_to_str(df_tgt):
        for col in df_tgt.columns:
            df_tgt[col] = df_tgt[col].astype('str')
            df_tgt[col] = df_tgt[col].str.replace('00:00:00', '').str.strip()
        
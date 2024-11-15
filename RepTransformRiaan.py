# Import libraries
import streamlit as st
import pandas as pd
from io import BytesIO
import base64
from sqlalchemy import create_engine, text
from datetime import datetime
import datetime as dt

def to_excel(df_final):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_final.to_excel(writer, sheet_name='Data', index=False)
    processed_data = output.getvalue()
    return processed_data

def get_table_download_link(df_final, date_end, report_type, filename="transformed_data.xlsx"):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    val = to_excel(df_final)
    b64 = base64.b64encode(val).decode()  # Some strings <-> bytes conversions necessary here
    formatted_date = date_end.strftime('%Y-%m-%d')
    formatted_filename = f"{formatted_date}_{report_type}_{filename}"
    return f'<a href="data:application/octet-stream;base64,{b64}" download="{formatted_filename}">Download Excel file</a>'

def append_data_to_sql(df, engine):
    table_name = 'fact_repsellout'
    database = 'tst_acorn'

    # Insert the new data
    df.to_sql(table_name, engine, if_exists='append', index=False)
    st.success(f"DataFrame written to table {table_name} in the {database} database.")

def df_stats(df, df_p, df_s):
    total_amount = df['Amount'].sum()
    
    total_units = df['Sell Out'].sum()

    st.write('**Total Sales:** ' "{:0,.0f}".format(total_amount).replace(',', ' '))
    st.write('')

    st.write('**Total Number of units sold:** ' "{:0,.0f}".format(total_units).replace(',', ' '))
    st.write('')
    st.write('**Top 10 products sold by amount:**')
    grouped_df_pt = df_p.groupby(["Product Description"]).agg({"Amount": "sum", "Sell Out": "sum"}).sort_values("Amount", ascending=False)
    grouped_df_final_pt = grouped_df_pt[['Sell Out','Amount']].head(10)
    st.table(grouped_df_final_pt.style.format({'Amount': '{:,.2f}', 'Sell Out': '{:,.0f}'}))
    st.write('')
    st.write('**Top 10 stores by amount:**')
    grouped_df_st = df_s.groupby("Retailer").agg({"Amount": "sum", "Sell Out": "sum"}).sort_values("Amount", ascending=False)
    grouped_df_final_st = grouped_df_st[['Sell Out','Amount']].head(10)
    st.table(grouped_df_final_st.style.format({'Amount': '{:,.2f}', 'Sell Out': '{:,.0f}'}))
    st.write('')
    st.write('**Final Dataframe:**')
    st.dataframe(df)

st.title('Rep Sell Out & Stock on Hand')

option = st.selectbox("Select the type of report:", ["Weekly Report", "Monthly Report"])

if option == "Weekly Report":

    brand = st.selectbox("Choose the brand:", ['Lexar','Sony'])

    if brand == 'Lexar':
        Date_End = st.date_input("Week ending: ")
        Date_Start = Date_End - dt.timedelta(days=6)
        WeekNumUse = st.number_input("Week to look at: ", min_value=0, max_value=9, step=1, format="%d")
        WeekNumUseStr = 'Week ' + str(int(WeekNumUse))
        st.write(f"The week we are looking at is: {WeekNumUseStr}")

        WeekNumCall = st.number_input("Week to call it: ", min_value=0, max_value=9, step=1, format="%d")
        WeekNumCallStr = 'Week ' + str(int(WeekNumCall))
        st.write(f"The week we are calling it is: {WeekNumCallStr}")

        st.write("")
        st.markdown("Please make sure the sheets in your file are named correctly as this will be used for the name of the rep")

        uploaded_files = st.file_uploader("Upload Rep Report", type="xlsx", accept_multiple_files=True)
        uploaded_pricelist = st.file_uploader("Upload Pricelist", type="xlsx")
        submit_button = st.button("Submit Weekly Report")

        if submit_button and uploaded_files and uploaded_pricelist:
            def transform_data(df):
                # Save the current header
                old_header = df.columns.tolist()

                # Insert the old header as the first row
                df.loc[-1] = old_header  # Add old header as a row at index -1
                df.index = df.index + 1  # Shift index
                df = df.sort_index()     # Sort index to move the new row to the top

                # Create new header with 'Unnamed:' prefix
                new_header = ['Unnamed: ' + str(i) for i in range(len(df.columns))]
                df.columns = new_header

                # Concatenate the first 4 rows with a delimiter '|'
                new_header = df.iloc[0:4].apply(lambda x: ' | '.join(x.dropna().astype(str)), axis=0)

                # Drop the first 4 rows and set new header
                df.columns = new_header
                df = df.iloc[4:].reset_index(drop=True)

                # Keep the first 3 columns
                id_vars = new_header[:3]

                # Unpivot the remaining columns
                melted_df = pd.melt(df, id_vars=id_vars, var_name='Variable', value_name='Value')

                filterdf_SOH = melted_df[~melted_df['Variable'].str.contains('Sell Out', na=False)]

                # Resetting index for filterdf_SOH
                filterdf_SOH = filterdf_SOH.reset_index(drop=True)

                filterdf_Sales = melted_df[~melted_df['Variable'].str.contains('Week', na=False)]

                # Resetting index for filterdf_Sales
                filterdf_Sales = filterdf_Sales.reset_index(drop=True)

                # Add 'Sales' from df2 to df1 using .loc
                filterdf = filterdf_SOH
                filterdf.loc[:, 'Sell Out'] = filterdf_Sales['Value']

                filterdf = filterdf[~filterdf['Variable'].str.contains('Notes', na=False)]

                # Rename columns
                df = filterdf.rename(columns={
                    'Unnamed: 0 | Category': 'Category',
                    'Unnamed: 1 | AX code': 'AX code',
                    'Unnamed: 2 | Product Description': 'Product Description',
                    'Unnamed: 3 | Date SOH was Collected: | Capacity': 'Capacity',
                    'Value': 'Stock on Hand'
                })

                # Split 'Variable' based on '|'
                df[['Retailer', 'Date SOH was Collected', 'Week No.', 'Dummy']] = df['Variable'].str.split('|', expand=True)

                # Drop 'Dummy' and 'Variable' columns
                df = df.drop(['Dummy', 'Variable'], axis=1)

                # Convert 'Sell Out' and 'Stock on Hand' column to integer
                df['Sell Out'] = pd.to_numeric(df['Sell Out'], errors='coerce').fillna(0).astype(int)
                df['Stock on Hand'] = pd.to_numeric(df['Stock on Hand'], errors='coerce').fillna(0).astype(int)

                # Strip spaces from 'Retailer' column
                df['Retailer'] = df['Retailer'].str.strip()

                # Strip spaces from 'Week' column
                df['Week No.'] = df['Week No.'].str.strip()

                # Remove dots and subsequent numbers, and then strip spaces from 'Retailer' column
                df["Retailer"] = df["Retailer"].str.replace(r"\.*\d+", "", regex=True)

                # Convert 'Date SOH was Collected' column to date type
                # df['Date SOH was Collected'] = pd.to_datetime(df['Date SOH was Collected']).dt.date

                return df

            all_transformed_dfs = []

            for uploaded_file in uploaded_files:
                all_sheets = pd.read_excel(uploaded_file, sheet_name=None)
                for sheet_name, df in all_sheets.items():
                    transformed_df = transform_data(df)
                    transformed_df['Rep'] = sheet_name  # Add the sheet name as the 'Rep' column
                    all_transformed_dfs.append(transformed_df)

            # Concatenate all transformed DataFrames
            final_df = pd.concat(all_transformed_dfs, ignore_index=True)
            
            # Filter out retailers containing "unnamed"
            final_df = final_df[~final_df['Retailer'].str.contains("unnamed", case=False, na=False)]
            
            # Filter data to include only the selected week number and call it the new week number
            final_df = final_df[final_df['Week No.'] == WeekNumUseStr]
            
            final_df['Week No.'] = WeekNumCallStr
            
            # Change the date to week ending
            final_df['Week Ending'] = Date_End
            final_df['Week Starting'] = Date_Start
            
            # Read the pricelist
            pricelist = pd.read_excel(uploaded_pricelist)
            pricelist = pricelist.rename(columns={'Dealer Excl' : 'Unit Price'})
            
            # Find the column with the word 'Dealer' in the pricelist
            # dealer_column = [col for col in pricelist.columns if 'Dealer' in col][0]

            # Convert all product codes to UPPER
            # pricelist['No.'] = pricelist['No.'].str.upper()
            final_df['AX code'] = final_df['AX code'].str.upper()

            # Merge with the pricelist
            final_df = final_df.merge(pricelist, left_on='AX code', right_on='No.', how='left')
            
            # Rename columns
            final_df = final_df.rename(columns={'Unit Price': 'Dealer Price'})

            # Identify products not on the pricelist
            products_not_on_pricelist = final_df[final_df['Dealer Price'].isna()][['AX code', 'Product Description', 'Stock on Hand', 'Sell Out']].drop_duplicates()
            products_not_on_pricelist_summary = products_not_on_pricelist.groupby(['AX code', 'Product Description']).agg({'Stock on Hand': 'sum', 'Sell Out': 'sum'}).reset_index()
            products_not_on_pricelist_summary = products_not_on_pricelist_summary[(products_not_on_pricelist_summary['Stock on Hand'] > 0) | (products_not_on_pricelist_summary['Sell Out'] > 0)]
            st.write("**Products not on the pricelist that have SOH or Sell Out:**")
            st.table(products_not_on_pricelist_summary)
            
            # Identify products without a price
            products_without_price = final_df[final_df['Dealer Price'].apply(lambda x: isinstance(x, str))][['AX code', 'Product Description', 'Stock on Hand', 'Sell Out']].drop_duplicates()
            products_without_price_summary = products_without_price.groupby(['AX code', 'Product Description']).agg({'Stock on Hand': 'sum', 'Sell Out': 'sum'}).reset_index()
            products_without_price_summary = products_without_price_summary[(products_without_price_summary['Stock on Hand'] > 0) | (products_without_price_summary['Sell Out'] > 0)]
            st.write("**Products without a price that have SOH or Sell Out:**")
            st.table(products_without_price_summary)

            # Identify duplicates
            duplicates = final_df.duplicated(subset=['AX code', 'Rep', 'Retailer', 'Week No.'], keep=False)
            duplicates_summary = final_df[duplicates][['AX code', 'Rep', 'Retailer', 'Week No.']].drop_duplicates()
            st.write("**This information is duplicated:**")
            st.table(duplicates_summary)

            # Convert Dealer Price to numeric, setting errors='coerce' to handle non-numeric values
            final_df['Dealer Price'] = pd.to_numeric(final_df['Dealer Price'], errors='coerce')

            # Calculate the Amount
            final_df['Amount'] = final_df['Sell Out'] * final_df['Dealer Price']

            # Add Date Created column with the current datetime
            final_df['Date Created'] = datetime.now()

            # Rename columns
            final_df = final_df.rename(columns={'AX code':'365 code'})
            final_df = final_df.rename(columns={'Capacity':'Category'})

            # Don't change these headings. Rather change the ones above
            final_df = final_df[['365 code', 'Product Description', 'Category', 'Rep','Brand Code','Item Group','Item Category Code','Inventory Posting Group','Model Class', 'Model Name','Model Classification','Week Starting','Week Ending', 'Retailer', 'Week No.', 'Stock on Hand', 'Sell Out', 'Dealer Price', 'Amount', 'Date Created']]
            final_df_p = final_df[['365 code', 'Product Description', 'Sell Out', 'Amount']]
            final_df_s = final_df[['Retailer', 'Sell Out', 'Amount']]

            # Show final df
            df_stats(final_df, final_df_p, final_df_s)

            st.markdown(get_table_download_link(final_df, Date_End, "Weekly_Lexar"), unsafe_allow_html=True)
        

    elif brand == 'Sony':
        Date_End = st.date_input("Week ending: ")
        Date_Start = Date_End - dt.timedelta(days=6)
        WeekNumUse = st.number_input("Week to look at: ", min_value=0, max_value=9, step=1, format="%d")
        WeekNumUseStr = 'Week ' + str(int(WeekNumUse))
        st.write(f"The week we are looking at is: {WeekNumUseStr}")

        WeekNumCall = st.number_input("Week to call it: ", min_value=0, max_value=9, step=1, format="%d")
        WeekNumCallStr = 'Week ' + str(int(WeekNumCall))
        st.write(f"The week we are calling it is: {WeekNumCallStr}")

        st.write("")
        st.markdown("Please make sure the sheets in your file are named correctly as this will be used for the name of the rep")

        uploaded_files = st.file_uploader("Upload Rep Report", type="xlsx", accept_multiple_files=True)
        uploaded_pricelist = st.file_uploader("Upload Pricelist", type="xlsx")
        submit_button = st.button("Submit Weekly Report")

        if submit_button and uploaded_files and uploaded_pricelist:
            def transform_data(df):
                # Save the current header
                old_header = df.columns.tolist()

                # Insert the old header as the first row
                df.loc[-1] = old_header  # Add old header as a row at index -1
                df.index = df.index + 1  # Shift index
                df = df.sort_index()     # Sort index to move the new row to the top

                # Create new header with 'Unnamed:' prefix
                new_header = ['Unnamed: ' + str(i) for i in range(len(df.columns))]
                df.columns = new_header

                # Concatenate the first 4 rows with a delimiter '|'
                new_header = df.iloc[0:4].apply(lambda x: ' | '.join(x.dropna().astype(str)), axis=0)

                # Drop the first 4 rows and set new header
                df.columns = new_header
                df = df.iloc[4:].reset_index(drop=True)

                # Keep the first 6 columns
                id_vars = new_header[:6]

                # Unpivot the remaining columns
                melted_df = pd.melt(df, id_vars=id_vars, var_name='Variable', value_name='Value')

                filterdf_SOH = melted_df[~melted_df['Variable'].str.contains('Sell Out', na=False)]

                # Resetting index for filterdf_SOH
                filterdf_SOH = filterdf_SOH.reset_index(drop=True)

                filterdf_Sales = melted_df[~melted_df['Variable'].str.contains('Week', na=False)]

                # Resetting index for filterdf_Sales
                filterdf_Sales = filterdf_Sales.reset_index(drop=True)

                # Add 'Sales' from df2 to df1 using .loc
                filterdf = filterdf_SOH
                filterdf.loc[:, 'Sell Out'] = filterdf_Sales['Value']

                filterdf = filterdf[~filterdf['Variable'].str.contains('Notes', na=False)]
                
                # Rename columns
                df = filterdf.rename(columns={
                    'Unnamed: 1 | 365 Code': '365 code',
                    'Unnamed: 2 | Product Description': 'Product Description',
                    'Unnamed: 3 | Category':'Category',
                    'Unnamed: 4 | Sub-Cat':'Sub-Cat',
                    'Unnamed: 5 | Date SOH was Collected: | Status': 'Status',
                    'Value': 'Stock on Hand'
                })

                # Split 'Variable' based on '|'
                df[['Retailer', 'Date SOH was Collected', 'Week No.', 'Dummy']] = df['Variable'].str.split('|', expand=True)

                # Drop 'Dummy' and 'Variable' columns
                df = df.drop(['Dummy', 'Variable'], axis=1)

                # Convert 'Sell Out' and 'Stock on Hand' column to integer
                df['Sell Out'] = pd.to_numeric(df['Sell Out'], errors='coerce').fillna(0).astype(int)
                df['Stock on Hand'] = pd.to_numeric(df['Stock on Hand'], errors='coerce').fillna(0).astype(int)

                # Strip spaces from 'Retailer' column
                df['Retailer'] = df['Retailer'].str.strip()

                # Strip spaces from 'Week' column
                df['Week No.'] = df['Week No.'].str.strip()

                # Remove dots and subsequent numbers, and then strip spaces from 'Retailer' column
                df["Retailer"] = df["Retailer"].str.replace(r"\.*\d+", "", regex=True)

                # Convert 'Date SOH was Collected' column to date type
                # df['Date SOH was Collected'] = pd.to_datetime(df['Date SOH was Collected']).dt.date

                return df

            all_transformed_dfs = []

            for uploaded_file in uploaded_files:
                all_sheets = pd.read_excel(uploaded_file, sheet_name=None)
                for sheet_name, df in all_sheets.items():
                    transformed_df = transform_data(df)
                    transformed_df['Rep'] = sheet_name  # Add the sheet name as the 'Rep' column
                    all_transformed_dfs.append(transformed_df)

            # Concatenate all transformed DataFrames
            final_df = pd.concat(all_transformed_dfs, ignore_index=True)

            # Filter out retailers containing "unnamed"
            final_df = final_df[~final_df['Retailer'].str.contains("unnamed", case=False, na=False)]
            
            # Filter data to include only the selected week number and call it the new week number
            final_df = final_df[final_df['Week No.'] == WeekNumUseStr]
            final_df['Week No.'] = WeekNumCallStr

            # Change the date to week ending
            final_df['Week Ending'] = Date_End
            final_df['Week Starting'] = Date_Start

            # Read the pricelist
            pricelist = pd.read_excel(uploaded_pricelist)
            pricelist = pricelist.rename(columns={'Dealer Excl':'Unit Price'})

            # Find the column with the word 'Dealer' in the pricelist
            # dealer_column = [col for col in pricelist.columns if 'Dealer' in col][0]

            # Convert all product codes to UPPER
            pricelist['No.'] = pricelist['No.'].str.upper()
            final_df['365 code'] = final_df['365 code'].str.upper()

            # Merge with the pricelist
            final_df = final_df.merge(pricelist, left_on='365 code', right_on='No.', how='left')
            
            # Rename columns
            final_df = final_df.rename(columns={'Unit Price': 'Dealer Price'})
            
            # Identify products not on the pricelist
            products_not_on_pricelist = final_df[final_df['Dealer Price'].isna()][['365 code', 'Product Description', 'Stock on Hand', 'Sell Out']].drop_duplicates()
            products_not_on_pricelist_summary = products_not_on_pricelist.groupby(['365 code', 'Product Description']).agg({'Stock on Hand': 'sum', 'Sell Out': 'sum'}).reset_index()
            products_not_on_pricelist_summary = products_not_on_pricelist_summary[(products_not_on_pricelist_summary['Stock on Hand'] > 0) | (products_not_on_pricelist_summary['Sell Out'] > 0)]
            st.write("**Products not on the pricelist that have SOH or Sell Out:**")
            st.table(products_not_on_pricelist_summary)

            # Identify products without a price
            products_without_price = final_df[final_df['Dealer Price'].apply(lambda x: isinstance(x, str))][['365 code', 'Product Description', 'Stock on Hand', 'Sell Out']].drop_duplicates()
            products_without_price_summary = products_without_price.groupby(['365 code', 'Product Description']).agg({'Stock on Hand': 'sum', 'Sell Out': 'sum'}).reset_index()
            products_without_price_summary = products_without_price_summary[(products_without_price_summary['Stock on Hand'] > 0) | (products_without_price_summary['Sell Out'] > 0)]
            st.write("**Products without a price that have SOH or Sell Out:**")
            st.table(products_without_price_summary)

            # Identify duplicates
            duplicates = final_df.duplicated(subset=['365 code', 'Rep', 'Retailer', 'Week No.'], keep=False)
            duplicates_summary = final_df[duplicates][['365 code', 'Rep', 'Retailer', 'Week No.']].drop_duplicates()
            st.write("**This information is duplicated:**")
            st.table(duplicates_summary)

            # Convert Dealer Price to numeric, setting errors='coerce' to handle non-numeric values
            final_df['Dealer Price'] = pd.to_numeric(final_df['Dealer Price'], errors='coerce')

            # Calculate the Amount
            final_df['Amount'] = final_df['Sell Out'] * final_df['Dealer Price']

            # Add Date Created column with the current datetime
            final_df['Date Created'] = datetime.now()

            # Don't change these headings. Rather change the ones above
            final_df = final_df[['365 code', 'Product Description', 'Category', 'Rep','Brand Code','Item Group','Item Category Code','Inventory Posting Group','Model Class', 'Model Name','Model Classification','Week Starting','Week Ending', 'Retailer', 'Week No.', 'Stock on Hand', 'Sell Out', 'Dealer Price', 'Amount', 'Date Created']]
            final_df_p = final_df[['365 code', 'Product Description', 'Sell Out', 'Amount']]
            final_df_s = final_df[['Retailer', 'Sell Out', 'Amount']]

            # Show final df
            df_stats(final_df, final_df_p, final_df_s)

            st.markdown(get_table_download_link(final_df, Date_End, "Weekly_Sony"), unsafe_allow_html=True)


    else: 
        st.write("Please select a brand")

elif option == "Monthly Report":
    Date_End = st.date_input("Month ending: ")
    uploaded_files = st.file_uploader("Choose Excel files", type="xlsx", accept_multiple_files=True)
    submit_button = st.button("Submit Monthly Report")

    if submit_button and uploaded_files:
        dfs_bino = []
        dfs_else = []

        for uploaded_file in uploaded_files:
            all_sheets = pd.read_excel(uploaded_file, sheet_name=None)
            df_bino = all_sheets.get('Bino')
            df_else = all_sheets.get('Everything Else')

            if df_bino is not None and df_else is not None:
                # Add a column for the file date (extracting date from the filename or setting a default date)
                df_bino['Date'] = df_bino['Week Ending']
                df_else['Date'] = df_else['Week Ending']
                dfs_bino.append(df_bino)
                dfs_else.append(df_else)

        if dfs_bino and dfs_else:
            # Concatenate all Bino DataFrames and Everything Else DataFrames
            df_bino = pd.concat(dfs_bino, ignore_index=True)
            df_else = pd.concat(dfs_else, ignore_index=True)

            # Sort by Date to ensure the latest Stock on Hand is used
            df_bino = df_bino.sort_values(by='Date')
            df_else = df_else.sort_values(by='Date')

            # Fill empty 'Sub-Cat' with a space " "
            df_bino['Sub-Cat'] = df_bino['Sub-Cat'].fillna(" ")
            df_else['Sub-Cat'] = df_else['Sub-Cat'].fillna(" ")

            # Aggregate Sell Out and keep the latest SOH and Dealer Price for each product and retailer
            df_bino = df_bino.groupby(['AX code', 'Product Description', 'Category', 'Capacity', 'Rep', 'Retailer']).agg(
                {'Sell Out': 'sum', 'Stock on Hand': 'last', 'Dealer Price': 'last'}).reset_index()
            df_else = df_else.groupby(['AX code', 'Product Description', 'Category', 'Capacity', 'Rep', 'Retailer']).agg(
                {'Sell Out': 'sum', 'Stock on Hand': 'last', 'Dealer Price': 'last'}).reset_index()

            # Calculate the Amount based on the aggregated Sell Out and Dealer Price
            df_bino['Amount'] = df_bino['Sell Out'] * df_bino['Dealer Price']
            df_else['Amount'] = df_else['Sell Out'] * df_else['Dealer Price']

            # Combine the Bino and Everything Else DataFrames for overall statistics
            final_df = pd.concat([df_bino, df_else], ignore_index=True)

            # Calculate the Amount
            final_df['Amount'] = final_df['Sell Out'] * final_df['Dealer Price']

            # Add Date Created column with the current datetime
            final_df['Date Created'] = datetime.now()

            final_df_p = final_df[['AX code', 'Product Description', 'Sell Out', 'Amount']]
            final_df_s = final_df[['Retailer', 'Sell Out', 'Amount']]

            # Show combined final df stats
            df_stats(final_df, final_df_p, final_df_s)
            
            # Add Month Ending to the DataFrame
            df_bino['Month Ending'] = Date_End
            df_else['Month Ending'] = Date_End

            # Reorder columns to match the weekly report
            df_bino = df_bino[['AX code', 'Product Description', 'Category', 'Capacity', 'Rep', 'Month Ending', 'Retailer', 'Stock on Hand', 'Sell Out', 'Dealer Price', 'Amount', 'Date Created']]
            df_else = df_else[['AX code', 'Product Description', 'Category', 'Capacity', 'Rep', 'Month Ending', 'Retailer', 'Stock on Hand', 'Sell Out', 'Dealer Price', 'Amount', 'Date Created']]

            # Provide the download link for the monthly report
            st.markdown(get_table_download_link(df_bino, df_else, Date_End, "Monthly"), unsafe_allow_html=True)
        else:
            st.write("Please ensure all uploaded files contain 'Bino' and 'Everything Else' sheets.")


elif option == 'Upload to SQL':
    uploaded_files = st.file_uploader("Choose Excel files", type="xlsx", accept_multiple_files=True)
    submit_button = st.button("Upload to SQL")

    if submit_button and uploaded_files:
        dfs_bino = []
        dfs_else = []

        for uploaded_file in uploaded_files:
            all_sheets = pd.read_excel(uploaded_file, sheet_name=None)
            df_bino = all_sheets.get('Bino')
            df_else = all_sheets.get('Everything Else')

            if df_bino is not None and df_else is not None:
                # Add a column for the file date (extracting date from the filename or setting a default date)
                df_bino['Date'] = df_bino['Week Ending']
                df_else['Date'] = df_else['Week Ending']
                dfs_bino.append(df_bino)
                dfs_else.append(df_else)

        if dfs_bino and dfs_else:
            # Concatenate all Bino DataFrames and Everything Else DataFrames
            df_bino = pd.concat(dfs_bino, ignore_index=True)
            df_else = pd.concat(dfs_else, ignore_index=True)

            # Sort by Date to ensure the latest Stock on Hand is used
            df_bino = df_bino.sort_values(by='Date')
            df_else = df_else.sort_values(by='Date')

            # Fill empty 'Sub-Cat' with a space " "
            df_bino['Sub-Cat'] = df_bino['Sub-Cat'].fillna(" ")
            df_else['Sub-Cat'] = df_else['Sub-Cat'].fillna(" ")

            # Calculate the Amount based on the aggregated Sell Out and Dealer Price
            df_bino['Amount'] = df_bino['Sell Out'] * df_bino['Dealer Price']
            df_else['Amount'] = df_else['Sell Out'] * df_else['Dealer Price']

            # Combine the Bino and Everything Else DataFrames for overall statistics
            final_df = pd.concat([df_bino, df_else], ignore_index=True)

            # Clean the 'Sub-Cat' column
            final_df['Sub-Cat'] = final_df['Sub-Cat'].apply(lambda x: None if pd.isna(x) or x.strip() == '' else x.strip())

            # Convert Dealer Price to numeric, setting errors='coerce' to handle non-numeric values
            final_df['Dealer Price'] = pd.to_numeric(final_df['Dealer Price'], errors='coerce')

            # Calculate the Amount
            final_df['Amount'] = final_df['Sell Out'] * final_df['Dealer Price']

            # Add Date Created column with the current datetime
            final_df['Date Created'] = datetime.now()

            # Don't change these headings. Rather change the ones above
            final_df = final_df[['AX code', 'Product Description', 'Category', 'Capacity', 'Rep', 'Week Ending', 'Retailer', 'Week No.', 'Stock on Hand', 'Sell Out', 'Dealer Price', 'Amount', 'Date Created']]

            # MySQL connection details
            user = 'tst_acorn'
            host = '13.244.79.93'
            password = 'tst_acorn123'
            port = '63036'
            database = 'tst_acorn'
            table_name = 'fact_repsellout'

            # Create the connection string
            connection_string = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'

            # Create an SQLAlchemy engine
            engine = create_engine(connection_string)

            # Append data to SQL
            append_data_to_sql(final_df, engine)

else:
    st.write("No report type selected")



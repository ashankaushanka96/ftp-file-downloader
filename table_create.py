import time

class TableData:
    def __init__(self, component_name, file_category, old_file, new_file, status, description):
        self.component_name = component_name
        self.file_category = file_category
        self.old_file = old_file
        self.new_file = new_file
        self.status = status
        self.description = description

def get_status_color(status):
    status_color = {
                'Updated': 'green',
                'Failed': 'red',
                'Not Updated': 'yellow',
            }.get(status, 'grey')
    
    return status_color


def create_table(table_data_list):
    table_rows = """"""
    today = time.strftime("%Y-%m-%d")

    index = 0

    while index < len(table_data_list):
        table_data = table_data_list[index]
        component_name = table_data.component_name
        file_category = table_data.file_category
        old_file = table_data.old_file
        new_file = table_data.new_file
        status = table_data.status
        description = table_data.description

        try:
            table_data_next = table_data_list[index+1]
            component_name_next = table_data_next.component_name
            file_category_next = table_data_next.file_category
            old_file_next = table_data_next.old_file
            new_file_next = table_data_next.new_file
            status_next = table_data_next.status
            description_next = table_data_next.description
        except:
            component_name_next = 'Null'

        if component_name == component_name_next:
            index = index + 2
            status_color = get_status_color(status)
            status_color_next = get_status_color(status_next)

            combined_files = f"EXISTING_LOCAL_FILE : {old_file} <br> FTP_LATEST_FILE : {new_file}"
            combined_files_next = f"EXISTING_LOCAL_FILE : {old_file_next} <br> FTP_LATEST_FILE : {new_file_next}"

            table_rows += f"""
            <tr>
                <td rowspan="3">{component_name}</td>
            </tr>
            <tr>    
                <td style="background-color: {status_color};">{file_category}</td>
                <td style="background-color: {status_color};">{combined_files}</td>
                <td style="background-color: {status_color};">{status}</td>
                <td style="background-color: {status_color};">{description}</td>
            </tr>
            <tr>    
                <td style="background-color: {status_color_next};">{file_category_next}</td>
                <td style="background-color: {status_color_next};">{combined_files_next}</td>
                <td style="background-color: {status_color_next};">{status_next}</td>
                <td style="background-color: {status_color_next};">{description_next}</td>
            </tr>
            """
        else:
            index = index + 1

            # Apply color directly to the "Status" column
            status_color = get_status_color(status)

            combined_files = f"EXISTING_LOCAL_FILE : {old_file} <br> FTP_LATEST_FILE : {new_file}"

            table_rows += f"""
            <tr>    
                <td>{component_name}</td>
                <td style="background-color: {status_color};">{file_category}</td>
                <td style="background-color: {status_color};">{combined_files}</td>
                <td style="background-color: {status_color};">{status}</td>
                <td style="background-color: {status_color};">{description}</td>
            </tr>
            """

        if index == len(table_data_list):
            break

    html_content = f"""
    <html>
    <head>
        <style>
            table {{ border-collapse: collapse; }}
            td, th {{ border: 1px solid black; padding: 5px; }}
        </style>
    </head>
    <body>
        <h2>iNAV Upload Status for {today}</h2>
        <table border="1" id="myTable">
        <tr>
            <th style="background-color: blue;">Component Name</th>
            <th style="background-color: blue;">File</th>
            <th style="background-color: blue;">Old File -> New File</th>
            <th style="background-color: blue;">Status</th>
            <th style="background-color: blue;">Description</th>
        </tr>
        {table_rows}
        </table>
    </body>
    </html>
    """

    return html_content

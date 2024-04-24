'''DOC2DEL module to give the common routes all over the modules'''
import os
import os.path
import json
from flask import Blueprint,request
import pandas as pd
from werkzeug.utils import secure_filename
from module_common import df_from_path, getchunkforPreview
from module_common_PC import limitedDf_from_path, find_delimiter,listSourceNamesCommon,Top_three_percentage
from module_datalineage import dataLineageforcolumn
module_common_api = Blueprint('module_common_api',__name__)


@module_common_api.route('/api/getPreview', methods=['POST'])
def get_preview():
    '''DOC2DEL Function to quickly preview a dataframe of the file sent'''
    uploaded_files = request.files.getlist("file[]")
    source_file_name="testfile"
    content={}
    for file in uploaded_files:
        file_name = secure_filename(file.filename)
        if file_name != '':
            file_ext = os.path.splitext(file_name)[1]
            if file_ext in ['.csv', '.xlsx', '.xls', '.json', '.xml']:
                source_file_name=source_file_name+file_ext
                file.save(source_file_name)
                file_size = os.path.getsize(source_file_name)/(1024*1024)
                dli=[]
                try:
                    if file_ext==".csv":
                        try:
                            str_dem = find_delimiter(source_file_name)
                            if str_dem =="True":
                                if file_size>10:
                                    df= getchunkforPreview(source_file_name,50)
                                else:
                                    df= df_from_path(source_file_name)
                            else:
                                raise pd.errors.ParserError
                        except pd.errors.ParserError:
                            data_dict={}
                            content['errorMsg']= 'Upload File Not A Valid CSV File'
                            content['errorflag']= 'True'
                            content['errorCode']= '105'
                            return json.dumps(content, default=str)
                    else:
                        if file_size>10:
                            df= getchunkforPreview(source_file_name,50)
                        else:
                            df= df_from_path(source_file_name)
                    dli= list(df.columns.values)
                    if any ('Unnamed' in col for col in dli):
                        raise pd.errors.ParserError
                    if any(len(ele.strip()) == 0 for ele in dli):
                        raise pd.errors.ParserError
                    df.index = df.index + 2
                    res_df = df.where(pd.notnull(df), 'None')
                    data_dict = res_df.to_dict('index')
                except (pd.errors.ParserError, pd.errors.EmptyDataError):
                    data_dict={}
                    content['errorMsg']= 'Data Parsing Failed. Missing headers AND/OR empty columns'
                    content['errorflag']= 'True'
                    content['errorCode']= '105'
                    return json.dumps(content, default=str)
    content['sourcePreview']=data_dict
    content['sourceColumns']=dli
    json_string = json.dumps(content, default=str)
    return json_string

@module_common_api.route('/api/checkSourceNameAvailability', methods=['POST'])
def check_source_name_availability():
    '''DOC2DEL Function to check the function name is available already in the database'''
    content = request.get_json()
    source_data_name = content["sourceName"]
    list_source_names = listSourceNamesCommon()
    if source_data_name in list_source_names:
        content['errorMsg']= 'The source name already exists'
        content['errorflag']= 'True'
        content['errorCode']= '101'
    else:
        content['errorMsg']= ''
        content['errorflag']= 'False'
        content['errorCode']= ''
        content['success']= 'Okay'
    return json.dumps(content, default=str)

@module_common_api.route("/api/df_to_json_preview_v1", methods=["POST"])
def df_to_json_preview_v1():
    '''DOC2DEL Function to quickly preview a dataframe stored in the backend'''
    content = request.get_json()
    selected_columns= content["selectedColumns"]
    if "seeMoreEnabled" in content:
        see_more_enabled= content["seeMoreEnabled"]
    else:
        see_more_enabled= "No"
    source_list=[]
    for col in selected_columns:
        if dataLineageforcolumn(col):
            result=dataLineageforcolumn(col)
            source_path=(result[-1])["Details"]["name"]
            source_list.append(source_path)
    source_list_new = list(set(source_list))
    if len(source_list_new)==0:
        content={}
        content['errorMsg']= 'The business term doesnt mapped with any source yet!'
        content['errorflag']= 'True'
        content['errorCode']= '601'
        return json.dumps(content, default=str)
    source_path= source_list_new[0]
    if see_more_enabled=="YES":
        df=df_from_path(source_path)
    else:
        df = limitedDf_from_path(source_path,1000)
    if not "Error" in df.keys():
        res_df = df.where(pd.notnull(df), 'None')
        result={}
        result['Preview'] =  res_df.to_dict('index')
        result['Top'] = Top_three_percentage(df)
        json_string = json.dumps(result, default=str)
    else:
        return df
    return json_string

@module_common_api.route("/api/df_to_json_preview_v2", methods=["POST"])
def df_to_json_preview_v2():
    """Function to quickly preview a dataframe stored in the backend version 2"""
    content = request.get_json()
    selected_columns= content["selectedColumns"]
    if "seeMoreEnabled" in content:
        see_more_enabled= content["seeMoreEnabled"]
    else:
        see_more_enabled= "No"
    source_list=[]
    for each_mapp in selected_columns:
        col=each_mapp
        l=dataLineageforcolumn(col)
        if l:
            source_list.append((l[-1])["Details"]["name"])
    source_list_new = list(set(source_list))
    if len(source_list_new)==0:
        content={}
        content['errorMsg']= 'The business term doesnt mapped with any source yet!'
        content['errorflag']= 'True'
        content['errorCode']= '601'
        return json.dumps(content, default=str)
    source_path= source_list_new[0]
    if see_more_enabled=="YES":
        df=df_from_path(source_path)
    else:
        df = limitedDf_from_path(source_path,1000)
    if not "Error" in df.keys():
        res_df = df.where(pd.notnull(df), 'None')
        data_dict = res_df.to_dict('index')
        result={}
        result['Preview'] = data_dict
        result['Top'] = Top_three_percentage(df)
        json_string = json.dumps(result, default=str)
    else:
        return df
    return json_string

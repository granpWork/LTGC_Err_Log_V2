import os
from datetime import datetime
from os import path
from Utils import Utils

import numpy as np
import pandas as pd
import re
import sys


def checkEmail(email):
    regex = '^(^[ña-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)'

    # pass the regular expression
    # and the string in search() method
    if email != '':
        if (re.search(regex, email)):
            return True
        else:
            return False
    pass


def checkEmplyeeNumberEmpty(x):
    if x == '':
        return True
    else:
        return False


def checkNA(x):
    switcher = {
        'na': True,
        'NA': True,
        'Na': True,
        'n/a': True,
        'N/A': True,
        'N/a': True,
        'NONE': True,
        'none': True,
        'N-A': True,
        'N-a': True,
    }

    return switcher.get(x, 'False')


def validateCtrlNumber(x, Company):
    util = Utils()
    compCode = util.companyNameLookUpMethod(Company)

    regex = "\\b" + compCode + "_[A-Za-z0-9-]+[_]{1}[M|C]{1}[1-9]{1}[0-9]?$"
    regexPalex = "\\bPALEX_[A-Za-z0-9-]+[_]{1}[M|C]{1}[1-9]{1}[0-9]?$"
    x = x.strip()

    if x == 'None':
        return 'None'
    else:
        if compCode == 'PAL':
            if re.match(regex, x):
                return True
            elif re.match(regexPalex, x):
                return True
            else:
                return False
        else:
            if re.match(regex, x):
                return True
            else:
                return False


def getMergeFileValidateEmail(inFile_EMP, inFile_HH, excelLogPath):
    dict_header = {'Company Name': 'Company'}

    df_EMP = pd.read_excel(inFile_EMP, sheet_name='Eligible Population',
                           header=1, dtype=str, na_filter=False)

    df_HH = pd.read_excel(inFile_HH, sheet_name='Eligible Population',
                          header=1, dtype=str, na_filter=False)

    df_HH.rename(columns=dict_header, inplace=True)

    # set identifier
    df_EMP['File'] = "EE"
    df_EMP['ID2'] = df_EMP['ID'].apply(lambda x: "EE-" + str(x))

    df_HH['File'] = "HH"
    df_HH['ID2'] = df_HH['ID'].apply(lambda x: "HH-" + str(x))

    # EMP File Dont have CTRL Number - Now default None
    df_EMP['Control Number'] = 'None'

    # Merge two DF
    frames = [df_EMP, df_HH]
    df_master = pd.concat(frames)

    # Error Checker
    df_master['Is Email Duplicate'] = df_master.duplicated(subset="Email Address", keep=False)
    df_master['Is Valid Email'] = df_master['Email Address'].apply(lambda x: checkEmail(x))
    df_master['Is Email NA'] = df_master['Email Address'].apply(lambda x: checkNA(x))
    df_master['Is Control Number Format Valid'] = df_master.apply(
        lambda x: validateCtrlNumber(x['Control Number'], x['Company']), axis=1)
    df_master['Is Duplicate Emp Number'] = df_master.duplicated(subset="Employee Number", keep=False)
    df_master['Emp Number Is Empty'] = df_master['Employee Number'].apply(lambda x: checkEmplyeeNumberEmpty(x))
    df_master['Is Employee Number NA'] = df_EMP['Employee Number'].apply(lambda x: checkNA(x))

    # Selected Columns
    df_master = df_master[
        ['ID', 'ID2', 'File', 'Company', 'Email Address', 'Employee Number',
         'Control Number', 'Is Email Duplicate', 'Is Email NA', 'Is Valid Email', 'Emp Number Is Empty',
         'Is Control Number Format Valid', 'Is Duplicate Emp Number', 'Is Employee Number NA']]

    group = df_master.groupby('Company')

    for i, comp in group:
        filename = excelLogPath + "/" + i + "_Merged_EMPHH.xlsx"
        comp.to_excel(filename)

    return df_master


def getError_IsEmailDuplicate(filename, FilePath):
    companyName = filename.split('_Merged')[0];

    print(companyName + " running...")
    df = pd.read_excel(FilePath, dtype={'ID': str, 'ID2': str, 'File': str, 'Company': str, 'Email Address': str,
                                        'Employee Number': str, 'Control Number': str}, na_filter=False)

    util = Utils()
    errMsg = []
    companyCode = util.companyNameLookUpMethod(companyName)

    # Cleanup - Drop NA and empty Email Address
    df.drop(df.loc[df['Email Address'] == ''].index, inplace=True)
    df.drop(df.loc[df['Is Email NA'] == True].index, inplace=True)

    dupEmail = df.loc[df['Is Email Duplicate'] == True]

    # remove Duplicate to convert into List
    noDup = dupEmail.drop_duplicates(subset=['Email Address'])

    for email in noDup['Email Address'].tolist():
        id = dupEmail.loc[dupEmail['Email Address'] == email]

        # convert List to String
        new_list = [str(i) for i in id['ID2'].tolist()]
        idStr = ', '.join(new_list)

        if not len(new_list) <= 1:
            errMsg.append("Error: ID[ " + idStr + " ] - " + email + "")
            # print("Error: ID[ " + idStr + " ] - " + email + "")

    generateErrorLog(errMsg, companyCode, "EmailDuplicate")

    pass


def generateErrorLog(errMsg, companyCode, arg):
    util = Utils()

    # write
    if len(errMsg):
        util.createSubCompanyFolder(companyCode, outPath)
        f = open(
            outPath + "/" + companyCode + "/" + companyCode + "_" + arg + "_err_log_" + dateTime + ".txt",
            "w")
        for err in errMsg:
            f.writelines(err + "\n")

        errMsg.clear()
    pass


def getError_IsEmailInvalidFormat(filename, FilePath):
    companyName = filename.split('_Merged')[0];

    print(companyName + " running...")
    df = pd.read_excel(FilePath, dtype={'ID': str, 'ID2': str, 'File': str, 'Company': str, 'Email Address': str,
                                        'Employee Number': str, 'Control Number': str}, na_filter=False)

    util = Utils()
    errMsg = []
    companyCode = util.companyNameLookUpMethod(companyName)

    df = df.loc[df['Is Valid Email'] == False]

    # remove Duplicate to convert into List
    noDup = df.drop_duplicates(subset=['Email Address'])

    for email in noDup['Email Address'].tolist():
        id = df.loc[df['Email Address'] == email]

        # convert List to String
        new_list = [str(i) for i in id['ID2'].tolist()]
        idStr = ', '.join(new_list)

        errMsg.append("Error: ID[ " + idStr + " ] - " + email + "")

    generateErrorLog(errMsg, companyCode, "InvalidEmailFormat")

    pass


def getError_IsCtrlNumberValid(filename, FilePath):
    companyName = filename.split('_Merged')[0];

    print(companyName + " running...")
    df = pd.read_excel(FilePath, dtype={'ID': str, 'ID2': str, 'File': str, 'Company': str, 'Email Address': str,
                                        'Employee Number': str, 'Control Number': str}, na_filter=False)

    util = Utils()
    errMsg = []
    companyCode = util.companyNameLookUpMethod(companyName)

    df = df.loc[df['Is Control Number Format Valid'] == False]

    for j, row in df.iterrows():
        errMsg.append("Error: ID[ " + str(row['ID2']) + " ] - " + row['Control Number'])

    generateErrorLog(errMsg, companyCode, "InvalidEmailFormat")

    pass


def getError_IsEmployeeNumDup(filename, FilePath):
    companyName = filename.split('_Merged')[0];

    print(companyName + " running...")
    df = pd.read_excel(FilePath, dtype={'ID': str, 'ID2': str, 'File': str, 'Company': str, 'Email Address': str,
                                        'Employee Number': str, 'Control Number': str}, na_filter=False)

    util = Utils()
    errMsg = []
    companyCode = util.companyNameLookUpMethod(companyName)

    # Cleanup - Drop NA and empty Email Address
    df.drop(df.loc[df['Employee Number'] == ''].index, inplace=True)
    df.drop(df.loc[df['Is Employee Number NA'] == True].index, inplace=True)

    dupEmpNumber = df.loc[df['Is Duplicate Emp Number'] == True]

    # remove Duplicate to convert into List
    noDup = dupEmpNumber.drop_duplicates(subset=['Email Address'])

    for empNumber in noDup['Employee Number'].tolist():
        id = dupEmpNumber.loc[dupEmpNumber['Employee Number'] == empNumber]

        # convert List to String
        new_list = [str(i) for i in id['ID2'].tolist()]
        idStr = ', '.join(new_list)

        if not len(new_list) <= 1:
            errMsg.append("Error: ID[ " + idStr + " ] - " + empNumber)

    generateErrorLog(errMsg, companyCode, "DuplicateEmployeeNumber")

    pass


def getError_IsEmployeeNumBlank(filename, FilePath):
    companyName = filename.split('_Merged')[0];

    print(companyName + " running...")
    df = pd.read_excel(FilePath, dtype={'ID': str, 'ID2': str, 'File': str, 'Company': str, 'Email Address': str,
                                        'Employee Number': str, 'Control Number': str}, na_filter=False)

    util = Utils()
    errMsg = []
    companyCode = util.companyNameLookUpMethod(companyName)

    df = df.loc[df['Emp Number Is Empty'] == True]

    for j, row in df.iterrows():
        errMsg.append("Error: ID[ " + str(row['ID2']) + " ] ")

    generateErrorLog(errMsg, companyCode, "EmployeeNumberBlank")

    pass


def getError_IsEmployeeNumNA(filename, FilePath):
    companyName = filename.split('_Merged')[0];

    print(companyName + " running...")
    df = pd.read_excel(FilePath, dtype={'ID': str, 'ID2': str, 'File': str, 'Company': str, 'Email Address': str,
                                        'Employee Number': str, 'Control Number': str}, na_filter=False)

    util = Utils()
    errMsg = []
    companyCode = util.companyNameLookUpMethod(companyName)

    df = df.loc[df['Is Employee Number NA'] == True]

    # remove Duplicate to convert into List
    noDup = df.drop_duplicates(subset=['Employee Number'])

    for empNumber in noDup['Employee Number'].tolist():
        id = df.loc[df['Employee Number'] == empNumber]

        # convert List to String
        new_list = [str(i) for i in id['ID2'].tolist()]
        idStr = ', '.join(new_list)

        if not len(new_list) <= 1:
            errMsg.append("Error: ID[ " + idStr + " ] - " + empNumber)
            # print("Error: ID[ " + idStr + " ] - " + empNumber)

    generateErrorLog(errMsg, companyCode, "EmployeeNumberIsNA")

    pass


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)

    today = datetime.today()
    dateTime = today.strftime("%m_%d_%y_%H%M%S")

    inPath = r"/Users/ranperedo/Documents/Vaccine/LTGSplit_Err_log/in"
    outPath = r"/Users/ranperedo/Documents/Vaccine/LTGSplit_Err_log/out"
    excelLogPath = r"/Users/ranperedo/Documents/Vaccine/LTGSplit_Err_log/excel_log"

    inFileList = [
        "LTGC_CEIRMasterlist.xlsx",
        "HHLTGC_CEIRMasterlist.xlsx"
    ]

    inFile_EMP = inPath + "/LTGC_CEIRMasterlist.xlsx"
    inFile_HH = inPath + "/HHLTGC_CEIRMasterlist.xlsx"

    print("==============================================================")
    print("Running Scpirt: V2 Error Finder LTGC Master List EMP and HH......")
    print("==============================================================")

    # merge infile - get email duplocate - email format - N/A
    getMergeFileValidateEmail(inFile_EMP, inFile_HH, excelLogPath)

    # Get all filenames in excelLogPath
    arrFilenames = os.listdir(excelLogPath)

    for filename in arrFilenames:
        FilePath = os.path.join(excelLogPath, filename)

        # if filename == "PMFTC_Merged_EMPHH.xlsx":
        #     # getError_IsEmailDuplicate(filename, FilePath)
        #     # getError_IsEmailInvalidFormat(filename, FilePath)
        #     # getError_IsCtrlNumberValid(filename, FilePath)
        #     # getError_IsEmployeeNumDup(filename, FilePath)
        #     # getError_IsEmployeeNumBlank(filename, FilePath)
        #     # getError_IsEmployeeNumNA(filename, FilePath)
        if not filename == ".DS_Store":
            getError_IsEmailDuplicate(filename, FilePath)
            getError_IsEmailInvalidFormat(filename, FilePath)
            getError_IsCtrlNumberValid(filename, FilePath)
            getError_IsEmployeeNumDup(filename, FilePath)
            getError_IsEmployeeNumBlank(filename, FilePath)
            getError_IsEmployeeNumNA(filename, FilePath)

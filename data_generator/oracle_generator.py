import random

import cx_Oracle


class Oracle:
    def __init__(self, args, logger):
        self.logger = logger
        self.logger.warn("Currently only the table 'DIMCUSTOMER' in dataset "
                         "AdventureWorks (link: https://github.com/artofbi/Oracle-AdventureWorks) is supported")
        self.connection = self.__init_connection(args)
        self.cursor = self.__get_cursor()
        self.schema = args.schema
        self.table = args.table
        if args.ids:
            self.remove_ids(args.ids)

    @staticmethod
    def __init_connection(args):
        return cx_Oracle.connect(args.username,
                                 args.password,
                                 cx_Oracle.makedsn(args.host, args.port, args.database),
                                 encoding='UTF-8')

    def __get_cursor(self):
        return self.connection.cursor()

    def close_connection(self):
        self.connection.close()

    def __run_query(self, sql_query):
        return self.cursor.execute(sql_query)

    def get_dimcustomer(self, column_values):
        sql_query = f"SELECT MAX(CUSTOMERKEY) from \"{self.schema}\".\"{self.table}\""
        column_values['MaxCUSTOMERKEY'] = self.__run_query(sql_query).fetchone()[0]

        sql_query = f"SELECT DISTINCT(GEOGRAPHYKEY) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE GEOGRAPHYKEY IS NOT NULL ORDER BY 1"
        column_values['GEOGRAPHYKEYs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CUSTOMERALTERNATEKEY) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE CUSTOMERALTERNATEKEY IS NOT NULL ORDER BY 1"
        column_values['CUSTOMERALTERNATEKEYs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(TITLE) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE TITLE IS NOT NULL ORDER BY 1"
        column_values['TITLEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(NAMESTYLE) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE NAMESTYLE IS NOT NULL ORDER BY 1"
        column_values['NAMESTYLEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(MARITALSTATUS) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE MARITALSTATUS IS NOT NULL ORDER BY 1"
        column_values['MARITALSTATUSs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(GENDER) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE GENDER IS NOT NULL ORDER BY 1"
        column_values['GENDERs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(EMAILADDRESS) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE EMAILADDRESS IS NOT NULL ORDER BY 1"
        column_values['EMAILADDRESSs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(YEARLYINCOME) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE YEARLYINCOME IS NOT NULL ORDER BY 1"
        column_values['YEARLYINCOMEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(TOTALCHILDREN) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE TOTALCHILDREN IS NOT NULL ORDER BY 1"
        column_values['TOTALCHILDRENs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(NUMBERCHILDRENATHOME) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE NUMBERCHILDRENATHOME IS NOT NULL ORDER BY 1"
        column_values['NUMBERCHILDRENATHOME'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ENGLISHEDUCATION) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE ENGLISHEDUCATION IS NOT NULL ORDER BY 1"
        column_values['ENGLISHEDUCATIONs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ENGLISHOCCUPATION) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE ENGLISHOCCUPATION IS NOT NULL ORDER BY 1"
        column_values['ENGLISHOCCUPATIONs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(HOUSEOWNERFLAG) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE HOUSEOWNERFLAG IS NOT NULL ORDER BY 1"
        column_values['HOUSEOWNERFLAGs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(NUMBERCARSOWNED) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE NUMBERCARSOWNED IS NOT NULL ORDER BY 1"
        column_values['NUMBERCARSOWNEDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(COMMUTEDISTANCE) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE COMMUTEDISTANCE IS NOT NULL ORDER BY 1"
        column_values['COMMUTEDISTANCEs'] = self.__run_query(sql_query).fetchall()

        return column_values

    def get_dimemployee(self, column_values):
        sql_query = f"SELECT MAX(EMPLOYEEKEY) from \"{self.schema}\".\"{self.table}\""
        column_values['MaxEMPLOYEEKEY'] = self.__run_query(sql_query).fetchone()[0]

        sql_query = f"SELECT DISTINCT(PARENTEMPLOYEEKEY) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE PARENTEMPLOYEEKEY IS NOT NULL ORDER BY 1"
        column_values['PARENTEMPLOYEEKEYs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(EMPLOYEENATIONALIDALTERNATEKEY) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE EMPLOYEENATIONALIDALTERNATEKEY IS NOT NULL ORDER BY 1"
        column_values['EMPLOYEENATIONALIDALTERNATEKEYs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(PARENTEMPLOYEENATIONALIDALTKEY) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE PARENTEMPLOYEENATIONALIDALTKEY IS NOT NULL ORDER BY 1"
        column_values['PARENTEMPLOYEENATIONALIDALTKEYs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(SALESTERRITORYKEY) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE SALESTERRITORYKEY IS NOT NULL ORDER BY 1"
        column_values['SALESTERRITORYKEYs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(FIRSTNAME) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE FIRSTNAME IS NOT NULL ORDER BY 1"
        column_values['FIRSTNAMEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(TITLE) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE TITLE IS NOT NULL ORDER BY 1"
        column_values['TITLEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(SALARIEDFLAG) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE SALARIEDFLAG IS NOT NULL ORDER BY 1"
        column_values['SALARIEDFLAGs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(PAYFREQUENCY) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE PAYFREQUENCY IS NOT NULL ORDER BY 1"
        column_values['PAYFREQUENCYs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(BASERATE) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE BASERATE IS NOT NULL ORDER BY 1"
        column_values['BASERATEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(VACATIONHOURS) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE VACATIONHOURS IS NOT NULL ORDER BY 1"
        column_values['VACATIONHOURSs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(SICKLEAVEHOURS) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE SICKLEAVEHOURS IS NOT NULL ORDER BY 1"
        column_values['SICKLEAVEHOURSs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(DEPARTMENTNAME) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE DEPARTMENTNAME IS NOT NULL ORDER BY 1"
        column_values['DEPARTMENTNAMEs'] = self.__run_query(sql_query).fetchall()

        return column_values

    def get_column_values(self):
        column_values = {}
        if self.table.lower() == 'dimcustomer':
            self.get_dimcustomer(column_values)
        elif self.table.lower() == 'dimemployee':
            self.get_dimemployee(column_values)

        return column_values

    @staticmethod
    def set_dimcustomer(columns, loop_counter, fake):
        random.seed(a=loop_counter, version=2)
        row = {
            'CUSTOMERKEY': str(columns['MaxCUSTOMERKEY'] + loop_counter),
            'GEOGRAPHYKEY': str(random.choice(columns['GEOGRAPHYKEYs'])[0]),
            'TITLE': '',
            # 'TITLE': str(random.choice(columns['TITLEs'])[0]),
            'FIRSTNAME': fake.first_name(),
            'MIDDLENAME': random.choice([fake.random_uppercase_letter(), '']),
            'LASTNAME': fake.last_name(),
            'NAMESTYLE': str(random.choice(columns['NAMESTYLEs'])[0]),
            'BIRTHDATE': fake.date_of_birth().strftime("%d-%b-%y 12:%M:%S.%f"),
            'MARITALSTATUS': str(random.choice(columns['MARITALSTATUSs'])[0]),
            # 'SUFFIX': str(random.choice(columns['SUFFIXs'])[0]),
            'SUFFIX': '',
            'GENDER': str(random.choice(columns['GENDERs'])[0]),
            'YEARLYINCOME': str(random.choice(columns['YEARLYINCOMEs'])[0]),
            'TOTALCHILDREN': str(random.choice(columns['TOTALCHILDRENs'])[0]),
            'NUMBERCHILDRENATHOME': str(random.choice(columns['NUMBERCHILDRENATHOME'])[0]),
            'ENGLISHEDUCATION': str(random.choice(columns['ENGLISHEDUCATIONs'])[0]),
            'ENGLISHOCCUPATION': str(random.choice(columns['ENGLISHOCCUPATIONs'])[0]),
            'HOUSEOWNERFLAG': str(random.choice(columns['HOUSEOWNERFLAGs'])[0]),
            'NUMBERCARSOWNED': str(random.choice(columns['NUMBERCARSOWNEDs'])[0]),
            'ADDRESSLINE1': fake.street_address(),
            'ADDRESSLINE2': '',
            'PHONE': fake.msisdn(),
            'DATEFIRSTPURCHASE': fake.date('%d-%b-%y 12:%M:%S.%f'),
            'COMMUTEDISTANCE': str(random.choice(columns['COMMUTEDISTANCEs'])[0]),
        }
        row['CUSTOMERALTERNATEKEY'] = f"AW{int(row['CUSTOMERKEY']):08d}"
        row['EMAILADDRESS'] = str(row['FIRSTNAME']).lower() + str(random.choice(range(1, 101))) + "@adventure-works.com"
        return row

    @staticmethod
    def set_dimemployee(columns, loop_counter, fake):
        random.seed(a=loop_counter, version=2)
        row = {
            'EMPLOYEEKEY': str(columns['MaxEMPLOYEEKEY'] + loop_counter),
            'PARENTEMPLOYEEKEY': str(random.choice(columns['PARENTEMPLOYEEKEYs'])[0]),
            'EMPLOYEENATIONALIDALTERNATEKEY': str(random.choice(columns['EMPLOYEENATIONALIDALTERNATEKEYs'])[0]),
            'PARENTEMPLOYEENATIONALIDALTKEY': str(random.choice(columns['PARENTEMPLOYEENATIONALIDALTKEYs'])[0]),
            'SALESTERRITORYKEY': str(random.choice(columns['SALESTERRITORYKEYs'])[0]),
            'FIRSTNAME': fake.first_name(),
            'LASTNAME': fake.last_name(),
            'MIDDLENAME': random.choice([fake.random_uppercase_letter(), '']),
            'NAMESTYLE': 0,
            'TITLE': str(random.choice(columns['TITLEs'])[0]),
            'HIREDATE': fake.date_of_birth().strftime("%y%m%d"),
            'BIRTHDATE': fake.date_of_birth().strftime("%y%m%d"),
            'PHONE': fake.msisdn(),
            'MARITALSTATUS': str(random.choice(['M', 'S'])[0]),
            'EMERGENCYCONTACTNAME': f"{fake.first_name()} {fake.last_name()}",
            'EMERGENCYCONTACTPHONE': fake.msisdn(),
            'SALARIEDFLAG': str(random.choice(columns['SALARIEDFLAGs'])[0]),
            'GENDER': str(random.choice(['M', 'F'])[0]),
            'PAYFREQUENCY': str(random.choice(columns['PAYFREQUENCYs'])[0]),
            'BASERATE': str(random.choice(columns['BASERATEs'])[0]),
            'VACATIONHOURS': str(random.choice(columns['VACATIONHOURSs'])[0]),
            'SICKLEAVEHOURS': str(random.choice(columns['SICKLEAVEHOURSs'])[0]),
            'CURRENTFLAG': 1,
            'SALESPERSONFLAG': 0,
            'DEPARTMENTNAME': str(random.choice(columns['DEPARTMENTNAMEs'])[0]),
            'STARTDATE': fake.date_of_birth().strftime("%y%m%d"),
            'STATUS': 'CURRENT',
        }
        row['LOGINID'] = f"adventure-works\{row['FIRSTNAME']}{str(random.choice(range(1, 101)))}"
        row['EMAILADDRESS'] = str(row['FIRSTNAME']).lower() + str(random.choice(range(1, 101))) + "@adventure-works.com"
        return row

    def set_column_values(self, columns, loop_counter, fake):
        row = {}
        if self.table.lower() == 'dimcustomer':
            row = self.set_dimcustomer(columns, loop_counter, fake)
        elif self.table.lower() == 'dimemployee':
            row = self.set_dimemployee(columns, loop_counter, fake)

        return row

    def query_dimcustomer(self, row):
        sql_query = f"""
        INSERT INTO \"{self.schema}\".\"{self.table}\" 
        (CUSTOMERKEY, GEOGRAPHYKEY, CUSTOMERALTERNATEKEY, TITLE, FIRSTNAME, MIDDLENAME, LASTNAME, NAMESTYLE, 
        BIRTHDATE, MARITALSTATUS, SUFFIX, GENDER, EMAILADDRESS, YEARLYINCOME, TOTALCHILDREN, 
        NUMBERCHILDRENATHOME, ENGLISHEDUCATION, ENGLISHOCCUPATION, HOUSEOWNERFLAG, NUMBERCARSOWNED, ADDRESSLINE1, 
        ADDRESSLINE2, PHONE, DATEFIRSTPURCHASE, COMMUTEDISTANCE) 
        VALUES 
        ({row['CUSTOMERKEY']}, {row['GEOGRAPHYKEY']}, '{row['CUSTOMERALTERNATEKEY']}', 
        '{row['TITLE']}', '{row['FIRSTNAME']}', 
        '{row['MIDDLENAME']}', 
        '{row['LASTNAME']}', '{row['NAMESTYLE']}', TO_TIMESTAMP('{row['BIRTHDATE']}', 'DD-MON-RR HH.MI.SS.FF'), 
        '{row['MARITALSTATUS']}', '{row['SUFFIX']}', 
        '{row['GENDER']}', '{row['EMAILADDRESS']}', '{row['YEARLYINCOME']}', '{row['TOTALCHILDREN']}', 
        '{row['NUMBERCHILDRENATHOME']}', '{row['ENGLISHEDUCATION']}', '{row['ENGLISHOCCUPATION']}', 
        '{row['HOUSEOWNERFLAG']}', '{row['NUMBERCARSOWNED']}', '{row['ADDRESSLINE1']}', '{row['ADDRESSLINE2']}', 
        '{row['PHONE']}', TO_TIMESTAMP('{row['DATEFIRSTPURCHASE']}', 'DD-MON-RR HH.MI.SS.FF'), '{row['COMMUTEDISTANCE']}')
        """
        return sql_query

    def query_dimemployee(self, row):
        sql_query = f"""
        INSERT INTO \"{self.schema}\".\"{self.table}\" 
        (EMPLOYEEKEY, PARENTEMPLOYEEKEY, EMPLOYEENATIONALIDALTERNATEKEY, PARENTEMPLOYEENATIONALIDALTKEY, 
        SALESTERRITORYKEY, FIRSTNAME, LASTNAME, MIDDLENAME, NAMESTYLE, TITLE, HIREDATE, BIRTHDATE, LOGINID, 
        EMAILADDRESS, PHONE, MARITALSTATUS, EMERGENCYCONTACTNAME, EMERGENCYCONTACTPHONE, SALARIEDFLAG, GENDER,
        PAYFREQUENCY, BASERATE, VACATIONHOURS, SICKLEAVEHOURS, CURRENTFLAG, SALESPERSONFLAG, DEPARTMENTNAME, 
        STARTDATE, STATUS) 
        VALUES 
        ({row['EMPLOYEEKEY']}, {row['PARENTEMPLOYEEKEY']}, '{row['EMPLOYEENATIONALIDALTERNATEKEY']}', 
        '{row['PARENTEMPLOYEENATIONALIDALTKEY']}', {row['SALESTERRITORYKEY']}, '{row['FIRSTNAME']}', 
        '{row['LASTNAME']}', '{row['MIDDLENAME']}', {row['NAMESTYLE']}, '{row['TITLE']}', {row['HIREDATE']}, 
        {row['BIRTHDATE']}, '{row['LOGINID']}', '{row['EMAILADDRESS']}', '{row['PHONE']}', '{row['MARITALSTATUS']}', 
        '{row['EMERGENCYCONTACTNAME']}', '{row['EMERGENCYCONTACTPHONE']}', {row['SALARIEDFLAG']}, '{row['GENDER']}', 
        {row['PAYFREQUENCY']}, {row['BASERATE']}, {row['VACATIONHOURS']}, {row['SICKLEAVEHOURS']}, 
        {row['CURRENTFLAG']}, {row['SALESPERSONFLAG']}, '{row['DEPARTMENTNAME']}', {row['STARTDATE']}, 
        '{row['STATUS']}')
        """
        return sql_query

    def generate_query(self, row):
        sql_query = ""
        if self.table.lower() == 'dimcustomer':
            sql_query = self.query_dimcustomer(row)
        elif self.table.lower() == 'dimemployee':
            sql_query = self.query_dimemployee(row)

        return sql_query

    def insert_and_commit(self, sql_query):
        self.__run_query(sql_query)
        self.connection.commit()

    def remove_ids(self, ids):
        sql_query = f"DELETE FROM \"{self.schema}\".\"{self.table}\" WHERE CUSTOMERKEY > {ids}"
        self.__run_query(sql_query)

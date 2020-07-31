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

    def get_column_values(self):
        column_values = {}
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

        sql_query = f"SELECT DISTINCT(FIRSTNAME) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE FIRSTNAME IS NOT NULL ORDER BY 1"
        column_values['FIRSTNAMEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(MIDDLENAME) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE MIDDLENAME IS NOT NULL ORDER BY 1"
        column_values['MIDDLENAMEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(LASTNAME) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE LASTNAME IS NOT NULL ORDER BY 1"
        column_values['LASTNAMEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(NAMESTYLE) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE NAMESTYLE IS NOT NULL ORDER BY 1"
        column_values['NAMESTYLEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(BIRTHDATE) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE BIRTHDATE IS NOT NULL ORDER BY 1"
        column_values['BIRTHDATEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(MARITALSTATUS) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE MARITALSTATUS IS NOT NULL ORDER BY 1"
        column_values['MARITALSTATUSs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(SUFFIX) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE SUFFIX IS NOT NULL ORDER BY 1"
        column_values['SUFFIXs'] = self.__run_query(sql_query).fetchall()

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

        sql_query = f"SELECT DISTINCT(ADDRESSLINE1) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE ADDRESSLINE1 IS NOT NULL ORDER BY 1"
        column_values['ADDRESSLINE1s'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ADDRESSLINE2) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE ADDRESSLINE2 IS NOT NULL ORDER BY 1"
        column_values['ADDRESSLINE2s'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(PHONE) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE PHONE IS NOT NULL ORDER BY 1"
        column_values['PHONEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(DATEFIRSTPURCHASE) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE DATEFIRSTPURCHASE IS NOT NULL ORDER BY 1"
        column_values['DATEFIRSTPURCHASEs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(COMMUTEDISTANCE) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE COMMUTEDISTANCE IS NOT NULL ORDER BY 1"
        column_values['COMMUTEDISTANCEs'] = self.__run_query(sql_query).fetchall()

        return column_values

    def generate_query(self, columns):
        return {}

    def insert_and_commit(self, sql_query):
        self.__run_query(sql_query)
        self.connection.commit()

    def remove_ids(self, ids):
        sql_query = f"DELETE FROM \"{self.schema}\".\"{self.table}\" WHERE CUSTOMERKEY > {ids}"
        self.__run_query(sql_query)

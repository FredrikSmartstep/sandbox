"""
Data handler for inserting and retrieving data from Database
"""

from os.path import join, dirname, abspath
import os
from sys import exc_info
import pandas as pd
import sqlalchemy as sqlal
from sqlalchemy.pool import NullPool
import sqlalchemy.exc as sqlal_exc
from sqlalchemy.sql import text
from sqlalchemy import event
from sqlalchemy import exc
from datetime import timedelta, date, datetime
import numpy as np
import yaml
import src.utils.custom_logger as customlogger
import time
from logger_tt import getLogger
from secret import secrets

log = getLogger(__name__)
class DataHandlerProduction:
    """
    Class for connecting to production database
    """

    def __init__(self, engine=None):
        if engine:
            log.info('Connecting using provided engine')
            self.engine = engine
        else: 
            mysql_config = 'mysql_config.yml'

            SQLALCHEMY_DATABASE_URI = None#os.getenv('SQLALCHEMY_DATABASE_URI') or None

            #print(SQLALCHEMY_DATABASE_URI)
            
            if SQLALCHEMY_DATABASE_URI: # Connect using env_var if it's initialized.
                temp = SQLALCHEMY_DATABASE_URI
                connect_text = "MYSQL CONNECTION using env key: SQLALCHEMY_DATABASE_URI"
            else:
                with open(join(dirname(abspath(__file__)), mysql_config), 'r') as ymlfile:
                    cfg = yaml.safe_load(ymlfile)
                    #cfg = yaml.load(ymlfile)
                    host = cfg['mysql']['host']
                    port = cfg['mysql']['port']
                    user = cfg['mysql']['user']
                    pw = secrets.mysql_pwd
                    self.dbschema = cfg['mysql']['db']

                    temp = r'mysql+pymysql://' + user + ':' + pw + '@' + host + ':' + str(port) + '/' + self.dbschema + "?charset=utf8mb4"
                    connect_text = "### MYSQL CONNECTION user=%s host=%s schema=%s" % (user, host, self.dbschema)

            # pool_pre_ping are used to make sure we have a connection before doing a query
            self.engine = sqlal.create_engine(temp, encoding='utf-8', echo=False, pool_pre_ping=True)
        # See https://docs.sqlalchemy.org/en/14/core/pooling.html#pooling-multiprocessing
        #self.engine.dispose() 
        self.connection = self.engine.connect()
        self._add_process_guards()
        log.info(connect_text)
    
    
    def _add_process_guards(self):
        """Add multiprocessing guards.
        Forces a connection to be reconnected if it is detected
        as having been shared to a sub-process.
        """

        @sqlal.event.listens_for(self.engine, "connect")
        def connect(dbapi_connection, connection_record):
            connection_record.info['pid'] = os.getpid()

        @sqlal.event.listens_for(self.engine, "checkout")
        def checkout(dbapi_connection, connection_record, connection_proxy):
            pid = os.getpid()
            if connection_record.info['pid'] != pid:
                log.exception('Database connection clash')
                connection_record.connection = connection_proxy.connection = None
                raise exc.DisconnectionError(
                    "Connection record belongs to pid %s, "
                    "attempting to check out in pid %s" %
                    (connection_record.info['pid'], pid)
                )
    
    def close(self):
        log.info('Closing DB connection')
        self.connection.close()
        self.engine.dispose()
        log.info('DB engine dispose executed')
        
    
    def get_external_account_names(self, idpatient, account_holder, service):
        query = text("""SELECT first_name, last_name FROM external_account 
                     WHERE idpatient= :p1 AND account_holder= :p2 
                     AND service= :p3""") 
        result = self.engine.execute(query, {'p1':idpatient, 'p2': account_holder, 'p3': service})
        if result.rowcount > 0:
            return result.fetchone()
        else:
            return None, None
        
    def update_external_account(self, idpatient, first_name, last_name, account_holder, service):
        query = text("""INSERT INTO external_account 
                     (idpatient, first_name, last_name, account_holder, service) 
                     VALUES (:p1, :p2, :p3, :p4, :p5)
                     ON DUPLICATE KEY UPDATE updated_at=NOW()""")
        result = self.engine.execute(query, {'p1': idpatient, 'p2': first_name, 'p3': last_name, 
                                             'p4': account_holder, 'p5': service})
        log.info('Update external account, ' + str(idpatient))
        
    def insert_meal_impact_values(self, meal_impact, df_meal_info, pat_id, duration=48):
        # Now add the meal impact to the DB, connecting the meal impact to a relevant meal_history instance
        # for each meal_starts
        # compare start time to times in meal_history
        # if close then amatch is found
        # write to the DB with this fk 
        # else skip
        df_meal_history = self.get_meal_history_between_dates(pat_id, df_meal_info.iloc[0].start_time - pd.Timedelta('1h'),
                            df_meal_info.iloc[-1].start_time + pd.Timedelta('1h'))
        df_meal_impact = pd.DataFrame()#columns=['meal_start','parameter_value_1', 'parameter_value_2', 'parameter_value_3', 'parameter_value_4', 'parameter_value_5',
                                       #           'parameter_value_6', 'parameter_value_7', 'parameter_value_8', 'parameter_value_9', 'parameter_value_10',
                                       #           'parameter_value_11', 'parameter_value_12', 'parameter_value_13', 'parameter_value_14', 'parameter_value_15', 'parameter_value_16', 'parameter_value_17', 'parameter_value_18', 'parameter_value_19',
                                       #           'parameter_value_20', 'parameter_value_21', 'parameter_value_22', 'parameter_value_23', 'parameter_value_24', 'parameter_value_25', 'parameter_value_26', 'parameter_value_27', 'parameter_value_28', 'parameter_value_29',
                                       #           'parameter_value_30', 'parameter_value_31', 'parameter_value_32', 'parameter_value_33', 'parameter_value_34', 'parameter_value_35', 'parameter_value_36', 'parameter_value_37', 'parameter_value_38', 'parameter_value_39',
                                       #           'parameter_value_40', 'parameter_value_41', 'parameter_value_42', 'parameter_value_43', 'parameter_value_44', 'parameter_value_45', 'parameter_value_46', 'parameter_value_47', 'parameter_value_48',
                                       #           'idrecipe','idpatient','idestimated_insulin_action'])
        
        pins, nf1, id0 = self.get_estimated_insulin_action(pat_id)
        row = 0
        for start_t, dur in zip(df_meal_info.start_time, df_meal_info.duration):
            if (dur>pd.Timedelta(30,'min')) & (dur<pd.Timedelta(300,'min')):
                params = meal_impact.loc[start_t:start_t + dur]
                N = len(params)-1
                df_meal_impact.loc[row, 'meal_start'] = pd.to_datetime(start_t)
                df_meal_impact.loc[row, 'idpatient'] = pat_id
                df_meal_impact.loc[row, 'idestimated_insulin_action'] = id0
                for ii in range(0, N):
                    # Add values for parameter_values
                    param_name = 'parameter_value_' + str(ii+1)
                    df_meal_impact.loc[row, param_name] = self.encode_float64(params[ii])  #encode np.float64 in case the encoder can't do it
                for ii in range(N, 48):
                    # Fill up
                    param_name = 'parameter_value_' + str(ii+1)
                    df_meal_impact.loc[row, param_name] = 0.0
                row = row + 1
        df_meal_impact.meal_start = pd.to_datetime(df_meal_impact.meal_start)
        df_meal_history = df_meal_history.sort_values(by='time_stamp')
        df_meal_history = df_meal_history.drop(axis=1, 
                                             columns=['idpatient',
                                                      'identified','meal_type', 
                                                      'source', 'calories', 
                                                      'carbohydrate', 'protein', 
                                                      'fat', 'alcohol', 
                                                      'portion_size', 'comment', 
                                                      'updated_at', 'created_at'])
        if (not df_meal_history.empty) & (not df_meal_impact.empty):
            df_meal_impact = pd.merge_asof(df_meal_impact, df_meal_history, left_on = 'meal_start', right_on = 'time_stamp',
                            direction='nearest', tolerance = pd.Timedelta('15min'))
            df_meal_impact = df_meal_impact.drop(axis=1, columns=['time_stamp', 'meal_start'])
            df_meal_impact = df_meal_impact[df_meal_impact['idmeal_history'].notna()]
            
            if (id0):
                self.to_sql(df_meal_impact, 'estimated_meal_impact', self.engine)
                log.info('INSERT into estimated_meal_impact, idpatient=%s' %pat_id)
            else:
                log.info('No insert of estimated_meal_impact, missing IA est, idpatient=%s' %pat_id)
        else:
            log.info('No insert of estimated_meal_impact, missing history or meal impact, idpatient=%s' %pat_id)
        return df_meal_impact

    def insert_insulin_action_values(self, insulin_action_value, idpatient):
        """
        Add insulin action values to database.
        Input:
            id_estimated_insulin_action - ID that we want to give our insulin action estimation
            insulin_action_value - array of values specifying insulin action profile
            patient_nr - patient ID
        Output:
            df_insulin_action - DataFrame of insulin action values, same as is written to database
        """
        log.debug('Adding insulin action')
        df_insulin_action = pd.DataFrame(columns=['idestimated_insulin_action','parameter_value_1', 'parameter_value_2', 'parameter_value_3', 'parameter_value_4', 'parameter_value_5',
                                                  'parameter_value_6', 'parameter_value_7', 'parameter_value_8', 'parameter_value_9', 'parameter_value_10',
                                                  'parameter_value_11', 'parameter_value_12', 'parameter_value_13', 'parameter_value_14', 'parameter_value_15', 'parameter_value_16', 'parameter_value_17', 'parameter_value_18', 'parameter_value_19',
                                                  'parameter_value_20', 'parameter_value_21', 'parameter_value_22', 'parameter_value_23', 'parameter_value_24', 'parameter_value_25', 'parameter_value_26', 'parameter_value_27', 'parameter_value_28', 'parameter_value_29',
                                                  'parameter_value_30', 'parameter_value_31', 'parameter_value_32', 'parameter_value_33', 'parameter_value_34', 'parameter_value_35', 'parameter_value_36', 'parameter_value_37', 'parameter_value_38', 'parameter_value_39',
                                                  'parameter_value_40', 'parameter_value_41', 'parameter_value_42', 'parameter_value_43', 'parameter_value_44', 'parameter_value_45', 'parameter_value_46', 'parameter_value_47', 'parameter_value_48', 'parameter_value_49',
                                                  'parameter_value_50', 'parameter_value_51', 'parameter_value_52', 'parameter_value_53', 'parameter_value_54', 'parameter_value_55', 'parameter_value_56', 'parameter_value_57', 'parameter_value_58', 'parameter_value_59',
                                                  'parameter_value_60', 'parameter_value_61', 'parameter_value_62', 'parameter_value_63', 'parameter_value_64', 'parameter_value_65', 'parameter_value_66', 'parameter_value_67', 'parameter_value_68', 'parameter_value_69',
                                                  'parameter_value_70', 'parameter_value_71', 'parameter_value_72', 'parameter_value_73', 'parameter_value_74', 'parameter_value_75', 'parameter_value_76', 'parameter_value_77', 'parameter_value_78', 'parameter_value_79',
                                                  'parameter_value_80', 'parameter_value_81', 'parameter_value_82', 'parameter_value_83', 'parameter_value_84', 'parameter_value_85', 'parameter_value_86', 'parameter_value_87', 'parameter_value_88', 'parameter_value_89',
                                                  'parameter_value_90',
                                                  'idinsulin','idpatient'])
        N = len(insulin_action_value)-1
        for ii in range(0, N):
            # Add values for parameter_values
            param_name = 'parameter_value_' + str(ii+1)
            df_insulin_action.loc[0, param_name] = self.encode_float64(insulin_action_value[ii,0])  #encode np.float64 in case the encoder can't do it
        for ii in range(N, 90):
            # Add values for parameter_values
            param_name = 'parameter_value_' + str(ii+1)
            df_insulin_action.loc[0, param_name] = 0.0

        id0 = self.get_first_available_id('estimated_insulin_action', 'idestimated_insulin_action')
        df_insulin_action.loc[0, 'idestimated_insulin_action'] = id0

        # get current insulin in use
        idinsulin = self.get_prescription(idpatient)
        if idinsulin is None:
            idinsulin = 6 # default to unknown rapid-acting
        df_insulin_action.loc[0, 'idinsulin'] = idinsulin
        df_insulin_action.loc[0, 'idpatient'] = idpatient
        self.to_sql(df_insulin_action, 'estimated_insulin_action', self.engine)

        return df_insulin_action
    
    def insert_generic_insulin_action(self, df_insulin_action):
        self.to_sql(df_insulin_action, 'estimated_insulin_action_generic', self.engine)

    def get_generic_insulin_action(self, idinsulin):
        query = """SELECT *
                   FROM estimated_insulin_action_generic
                   WHERE idinsulin=%s
                """ % idinsulin
        df_pins = self.read_sql_query(query)

        if df_pins.empty:
            return np.matrix([]).T, 0, None

        s = df_pins.loc[:, df_pins.columns.str.contains('parameter_value_')].values
        pins = s[:, ~(s[0, :] == 0)].T
        nf1 = len(pins)
        idestimated_insulin_action = df_pins.loc[0, 'idestimated_insulin_action_generic']

        return pins, nf1, idestimated_insulin_action

    def insert_gb0_values(self, gb0, indices, idestimated_insulin_action, idpatient):
        """
        Add baseline Gb values (one for each time interval or date) for a given patient and insulin action estimation
        Input:
            Gb0 - array of baseline Gb values, one for each time interval
            indices - list of lists of timestamps defining the time intervals from which the Gb0 values where estimated
            id_estimated_insulin_action - ID of insulin action estimation
            idpatient - ID of patient
        Output:
            df_Gb0_values - DataFrame of Gb0 values, same as is written to database
        """
        log.debug('Adding Gb0 values')
        table_name = 'estimated_gb0_values'

        df_Gb0_values2 = pd.DataFrame(columns=['start_time', 'end_time', 'Gb0', 'idestimated_insulin_action'])
        #df_Gb0_values = df_Gb0_values.astype({'start_time': np.datetime64, 'end_time': np.datetime64, 'Gb0':np.float, 'idestimated_insulin_action': int})
        number_param = len(gb0)
        for i in range(number_param):
            df_Gb0_values2.loc[i, 'start_time'] = datetime.strptime(str(indices[i][0]), '%Y-%m-%d %H:%M:%S')
            df_Gb0_values2.loc[i, 'end_time'] = datetime.strptime(str(indices[i][1]), '%Y-%m-%d %H:%M:%S')
            df_Gb0_values2.loc[i, 'Gb0'] = self.encode_float64(gb0[i])
            df_Gb0_values2.loc[i, 'idestimated_insulin_action'] = idestimated_insulin_action
            df_Gb0_values2.loc[i, 'idpatient'] = idpatient

        self.to_sql(df_Gb0_values2, table_name, self.engine)
        return df_Gb0_values2


    def insert_gb_beta_values(self, gb, ia_factor, time, idpatient, id_estimated_insulin_action):
        """
        Add time variables Gb and ia_factor to database for a given insulin action estimation.
        Will delete any previous entries for the same insulin action estimation.
        Input:
            Gb - array of time variability parameters of Gb value, also known as rho. Should be around 1.
            ia_factor - time variability parameters of IA, also known as beta. Should be around 1.
            time - time stamps for the different time variability parameters
            patient_nr - ID of patient
            id_estimated_insulin_action - ID of insulin action estimation
        Output:
            df_GbBeta - DataFrame of Gb and beta values, same as is written to database
        """
        print('Adding beta and rho to estimated_gb_ia_factor.')
        idestimated_Gb_IA_factor = self.get_first_available_id('estimated_gb_ia_factor', 'idestimated_Gb_IA_factor')

        df_GbBeta = pd.DataFrame(columns=['idestimated_Gb_IA_factor','gb', 'ia_factor',
                                          'time_stamp', 'idestimated_insulin_action','idpatient'])
        number_param = len(time)
        for ii in range(0, number_param):
            df_GbBeta.loc[ii,'ia_factor'] = self.encode_float64(ia_factor[ii])
            df_GbBeta.loc[ii,'gb'] = self.encode_float64(gb[ii])
            df_GbBeta.loc[ii,'time_stamp'] = time[ii]
            df_GbBeta.loc[ii,'idestimated_Gb_IA_factor'] = idestimated_Gb_IA_factor + ii

        df_GbBeta['idestimated_insulin_action'] = id_estimated_insulin_action
        df_GbBeta['idpatient'] = idpatient
        self.to_sql(df_GbBeta, 'estimated_gb_ia_factor', self.engine)

        query = text("""Select gb, ia_factor, time_stamp, idpatient
                        from estimated_gb_ia_factor
                        where idestimated_insulin_action = :p1
                     """)
        estimated_GbIAfactor = self.engine.execute(query, {'p1': int(id_estimated_insulin_action)})
        row = estimated_GbIAfactor.fetchall()

        nbroldentries = len(row) - df_GbBeta.shape[0]
        if df_GbBeta.shape[0] > 0 and nbroldentries > 0:
            delquery = text("""DELETE FROM estimated_gb_ia_factor
                               WHERE idestimated_insulin_action = :p1
                               ORDER BY created_at ASC LIMIT :p2
                            """)
            self.engine.execute(delquery, ({'p1': int(id_estimated_insulin_action),
                                            'p2': nbroldentries}))

        return df_GbBeta


    def insert_insulin_on_board(self, amount_remaining, idpatient, time):
        idiob = self.get_first_available_id('iob', 'idiob')
        df_insulin_on_board = pd.DataFrame(columns=['idiob', 'iob', 'time_stamp', 'idpatient'])
        df_insulin_on_board.loc[0, 'idiob'] = idiob
        df_insulin_on_board.loc[0, 'iob'] = self.encode_float64(amount_remaining)
        df_insulin_on_board.loc[0, 'time_stamp'] = time
        df_insulin_on_board.loc[0, 'idpatient'] = idpatient

        self.to_sql(df_insulin_on_board, 'iob', self.engine)
        log.info('INSERT into iob (insulin on board), idpatient=%s' % idpatient)


    def insert_meal_impact(self, pmeal, idpatient, recipe_ids, meal_ids, bolus_opt, corr_dose, meal_dose, idestimated_insulin_action):
        idemi = self.get_first_available_id('estimated_meal_impact', 'idestimated_meal_impact')
        nbr_meals, nbr_parameters = pmeal.shape

        df_meal_impact = pd.DataFrame(columns=['opt_bolus_now', 'opt_bolus_delayed',
                                               'correction_actual', 'meal_dose_actual',
                                               'correction_optimized', 'meal_dose_optimized'])
        df_meal_impact.loc[:, 'idestimated_meal_impact'] = np.arange(idemi, idemi + nbr_meals)
        for i in range(nbr_parameters):
            df_meal_impact.loc[:, 'parameter_value_{}'.format(i+1)] = [self.encode_float64(v) for v in pmeal[:, i]]
        df_meal_impact.loc[:, 'idrecipe'] = recipe_ids
        df_meal_impact.loc[:, 'idpatient'] = idpatient
        df_meal_impact.loc[:, 'idmeal_history'] = meal_ids
        df_meal_impact.loc[:, ['opt_bolus_now', 'opt_bolus_delayed']] = bolus_opt
        df_meal_impact.loc[:, ['correction_actual', 'correction_optimized']] = corr_dose
        df_meal_impact.loc[:, ['meal_dose_actual', 'meal_dose_optimized']] = meal_dose
        df_meal_impact.loc[:, 'idestimated_insulin_action'] = idestimated_insulin_action

        #Remove existing meal impacts with the same idmeal_history
        query = 'DELETE FROM estimated_meal_impact WHERE idpatient = {}'.format(idpatient)
        self.read_sql_query(query)
        log.info('DELETE from estimated_meal_implact where idpatient=%s')

        self.to_sql(df_meal_impact, 'estimated_meal_impact', self.engine)
        log.info('INSERT into estimated_meal_impact, idpatient=%s' %idpatient)
        
    def get_pipeline_status(self, idpatient, tag):
        query = """SELECT status
                   FROM pipeline_status
                   WHERE idpatient={} AND pipe="{}" 
                """.format(idpatient, tag) 
        result = self.engine.execute(query)
        result = result.fetchone()
        if result is not None:
            status = result[0]
            return status
        else:
            return False 
        
    def update_pipeline_status(self, idpatient, tag):
        query = text('INSERT INTO pipeline_status (idpatient, pipe, status) VALUES (:p1, :p2, :p3)')
        result = self.engine.execute(query, {'p1': idpatient, 'p2': tag, 'p3': True})
        log.info('Update pipeline status, '   + tag + ', ' + str(idpatient))
    
    def get_optimized_bolus(self, idpatient):
        query = """SELECT idmeal_history, opt_bolus_now, opt_bolus_delayed,
                   correction_actual, meal_dose_actual,
                   correction_optimized, meal_dose_optimized
                   FROM estimated_meal_impact
                   WHERE idpatient=%s
                """ % idpatient
        df_opt_bolus = self.read_sql_query(query)
        return df_opt_bolus

    def get_prescription(self, idpatient):
        query = "SELECT idinsulin FROM prescription WHERE idpatient=%s" % idpatient
        result = self.engine.execute(query)
        result = result.fetchone()
        if result is not None:
            idinsulin = result[0]
            return idinsulin
        else:
            return None 
        
    def get_active_basal_program(self, idpatient):
        query = text("""SELECT idtherapy_settings FROM therapy_settings 
                     WHERE therapy_settings_type=\'current\' AND active=1 AND idpatient= :p1""") 
        result = self.engine.execute(query, {'p1':idpatient})
        if result.rowcount > 0:
            idtherpy_settings = result.fetchone()[0]
        else:
            return None
        query = """SELECT start_time, value FROM therapy_settings_value 
                WHERE idtherapy_settings={} AND idpatient={} AND value_type=\'basal_program\'
                """.format(idtherpy_settings, idpatient)
        df_active_basal_program = self.read_sql_query(query)
        
        return df_active_basal_program
    
    def get_number_of_therapy_settings_profiles(self, idpatient):
        query = text("""SELECT MAX(profile) FROM therapy_settings_value WHERE idpatient= :p1""")
        
        return self.engine.execute(query, {'p1':idpatient}).fetchone()[0]
        
    def get_data_for_prediction(self, idpatient, end_date = '2000-01-01', calibs=True):
        # Get all data for selected patient
        # We decided to do separate queries and get separate dataframes.
        # Then, the data pre-processing class will take care of resampling etc
        
        # Protection against overflow - deafult to no more than 3 months
        if end_date=='2000-01-01':
            # get latest date
            end_date = self.get_timestamp_last_insert_patient_variables(idpatient, 2) 
            if not end_date:
                end_date = self.get_latest_bolus_time_stamp(idpatient)
            if end_date:
                end_date+= timedelta(days=-120)
            else:
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),\
                        pd.DataFrame(), pd.DataFrame()
            
        if not isinstance(end_date,str):
            end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")
        
        query = """SELECT time_stamp AS time_stamp_BG, value AS BG_value, idpatient
                   FROM variable_values
                   WHERE idpatient_variables = 1 AND idpatient = {} AND time_stamp> \'{}\'
                """.format(idpatient,end_date)        
        df_bg_history = self.read_sql_query(query)

        query = """SELECT time_stamp AS time_stamp_CGM, value AS CGM_value, idpatient
                   FROM variable_values
                   WHERE idpatient_variables = 2 AND idpatient = {} AND time_stamp> \'{}\'
                """.format(idpatient,end_date)
        df_cgm_history = self.read_sql_query(query)

        query = """SELECT time_stamp AS time_stamp_basal, basal_level AS basal, idpatient
                   FROM basal_change_history
                   WHERE idpatient = {} AND time_stamp> \'{}\'
                """.format(idpatient,end_date)
        df_basal_change_history = self.read_sql_query(query)

        query = """SELECT time_stamp AS time_stamp_bolus, amount AS bolus, amount_now, amount_later, duration, type, idmeal_history, idpatient, idinsulin, id_device
                FROM bolus_history
                WHERE idpatient = {} AND time_stamp> \'{}\'
            """.format(idpatient,end_date)

        df_bolus_history = self.read_sql_query(query)

        query = """SELECT meal_history.*, recipe.name
                   FROM meal_history
                   INNER JOIN recipe ON meal_history.idrecipe = recipe.idrecipe
                   WHERE meal_history.idpatient={} AND time_stamp> \'{}\'
                """.format(idpatient,end_date)
        df_meal_history = self.read_sql_query(query)
        if calibs:
            query = """SELECT time_stamp AS time_stamp_BG, value AS BG_value, idpatient
                   FROM variable_values
                   WHERE idpatient_variables = 260 AND idpatient = {} AND time_stamp> \'{}\'
                """.format(idpatient,end_date)        
            df_calibration_values = self.read_sql_query(query)
            return df_bg_history, df_cgm_history, df_basal_change_history, df_bolus_history, \
                df_meal_history, df_calibration_values

            
        return df_bg_history, df_cgm_history, df_basal_change_history, df_bolus_history, df_meal_history


    ### NO USAGE 2018-07-31
    def resample_bolus_history(self, df_bolus_history):
        main = df_bolus_history.loc[:,['time_stamp','amount']]
        main = main.set_index('time_stamp')
        final = main.resample('1Min').max() # final.index = time_stamp
        df_resampled_bolus_history = final.fillna(0,inplace = False)

        return df_resampled_bolus_history
    
    def get_patients_from_clinic(self, idclinic):
        result = self.engine.execute('SELECT idpatient FROM clinic_listing WHERE idclinic = %s' %idclinic).fetchall()
        li = [str(e[0]) for e in result]
        df = pd.DataFrame()
        if len(li):
            patient_list = (',').join(li)
            query = text("""SELECT * FROM patient WHERE idpatient IN (%s)""" %(patient_list))
            df = pd.read_sql_query(query,self.connection)
        return df
    
    def insert_patient_to_specific_clinic(self, last_name, first_name, social_security_nr, date_of_register, idclinic):
        # check if there already exists a patient with this name at this clinic
        # get list of patients for this clinic
        result = self.engine.execute('SELECT idpatient FROM clinic_listing WHERE idclinic = %s' %idclinic).fetchall()
        li = [str(e[0]) for e in result]
        df = pd.DataFrame()
        if len(li):
            patient_list = (',').join(li)
            query = text("""SELECT * FROM patient WHERE last_name = "%s"
                     AND first_name = "%s" AND idpatient IN (%s)""" %(last_name, first_name, patient_list))
            df = pd.read_sql_query(query,self.connection)
        # if match found
        if len(df):
            log.info('Matching names')
            # Check if social_security number matches
            patient = df[df.social_security_nr==social_security_nr].idpatient
            if (not patient.empty) & (social_security_nr is not None):
                idpatient = patient.iloc[0]
                log.info("Adding patient. Patient already exists. Idpatient: " + str(idpatient))
                return idpatient
            elif social_security_nr==None:
                log.info("Adding patient. Patient with this name already exists at this clinic. Aborting.")
                return None # matching name but since no soc sec nr was provided it is impossible to say whether there is a true match 
        # Check if patient exists for another clinic
        else: 
            if social_security_nr is not None:
                query = text("SELECT * FROM patient WHERE social_security_nr= :p1")
                result = self.engine.execute(query, {'p1':social_security_nr})
                if result.rowcount > 0:
                    idpatient = result.fetchone()[0]
                    log.info("Adding patient. Patient already exists at another clinic. idpatient: " + str(idpatient))
                    self.insert_patient_to_clinical_listing(idpatient, idclinic)
                    return idpatient
            else:
                log.info('Not adding patient without social security number')
                return None
        # Let's add the patient
        result = self.insert_patient(last_name, first_name, social_security_nr, date_of_register)
        idpatient = result.lastrowid
        # Add to clinic
        self.insert_patient_to_clinical_listing(idpatient, idclinic)
        return idpatient    

    def insert_patient(self, last_name, first_name, social_security_nr, date_of_register):
        query = text('INSERT INTO patient (last_name, first_name, social_security_nr, date_of_register) VALUES (:p1, :p2, :p3, :p4)')
        boo = self.engine.execute(query, ({'p1':last_name, 'p2': first_name, 'p3': social_security_nr, 'p4': date_of_register}))
        log.info('SQL INSERT PATIENT, LAST_NAME=%s, FIRST_NAME=%s, idpatient=%s' % (last_name, first_name, boo.lastrowid))
        return boo
    
    def insert_patient_to_clinical_listing(self, idpatient, clinic_id):
        query = text('INSERT INTO clinic_listing (idpatient, idclinic) VALUES (:p1, :p2)')
        result = self.engine.execute(query, {'p1': idpatient, 'p2': clinic_id})
        log.info('SQL INSERT INTO clinic_listing, idpatient=%s, idclinic=%s' % (idpatient, clinic_id))

    def get_estimated_insulin_action(self, idpatient):
        """
        This method retrieves the most recently calculated p_ins from the DB, for a specific patient of choice
        Input: idpatient
        Output:
            pins - array containing all the non zero values of pins
            nf1 - length of pins array
            idestimated_insulin_action - id of latest estimation in DB
        """

        query = """SELECT *
                   FROM estimated_insulin_action
                   WHERE idpatient = %s
                   ORDER BY updated_at DESC LIMIT 1
                """ % idpatient
        df_pins = self.read_sql_query(query)

        if df_pins.empty:
            return np.matrix([]).T, 0, None

        s = df_pins.loc[:, df_pins.columns.str.contains('parameter_value_')].values
        pins = s[:, ~(s[0, :] == 0)].T
        nf1 = len(pins)
        idestimated_insulin_action = df_pins.loc[0, 'idestimated_insulin_action']

        return pins, nf1, idestimated_insulin_action
    
    def get_all_estimated_insulin_action(self):
        """
        This method retrieves all estimated pins
        Output:
            pins - array containing all the values of pins
        """

        query = """SELECT *
                   FROM estimated_insulin_action
                """
        return self.read_sql_query(query)

    
    def get_estimated_gb0_values(self, idestimated_insulin_action):
        """
        Retrieves the most recently calculated Gb0 values from the DB for a specific patient
        Input: patient nr
        Output: array containing the Gb0 values
        """
        if idestimated_insulin_action == None:
            return np.array([]), np.array([]), np.array([])

        query = """SELECT *
                   FROM estimated_gb0_values
                   WHERE idestimated_insulin_action = %s
                """ % str(idestimated_insulin_action)
        df_Gb0 = self.read_sql_query(query)

        return df_Gb0['Gb0'].values, df_Gb0['start_time'].values, df_Gb0['end_time'].values
    
    def get_estimated_gb0_value_between_dates(self, idpatient, idestimated_insulin_action, 
                                              end_date='2000-01-01'):
        """
        Retrieves the most recently calculated Gb0 values from the DB for a specific patient
        Input: patient nr
        Output: array containing the Gb0 values
        """
        if end_date=='2000-01-01':
            # get latest date
            end_date = self.get_timestamp_last_insert_patient_variables(idpatient, 2) 
            if end_date:
                end_date+= timedelta(days=-30)
                
        if idestimated_insulin_action == None:
            return np.array([]), np.array([]), np.array([])

        query = """SELECT *
                   FROM estimated_gb0_values
                   WHERE idestimated_insulin_action = %s
                   AND start_time>= '%s'
                """ % (str(idestimated_insulin_action), end_date)
        df_Gb0 = self.read_sql_query(query)

        return df_Gb0['Gb0'].values, df_Gb0['start_time'].values, df_Gb0['end_time'].values
    
    ### NO USAGE 2018-07-31
    def get_estimated_IOB(self, idpatient):
        """
        This method retrieves the most recently calculated IOB from the DB, for a specific patient of choice
        Input: idpatient
        Output: iob_downsampled, percentage_remaining, amount_remaining
        """
        print('This method needs to be finalized once the table for IOB is in place in the DB')
        print('Patient nr: ' + str(idpatient))

        query = text('SELECT * from estimated_IOB WHERE idpatient = :p1')
        estimated_IOB = self.engine.execute(query, {'p1':idpatient}) # pointer
        row = estimated_IOB.fetchall() # actual values

        labels = ['idestimated_insulin_on_board','parameter_value_1', 'parameter_value_2', 'parameter_value_3', 'parameter_value_4', 'parameter_value_5','parameter_value_6', 'parameter_value_7', 'parameter_value_8', 'parameter_value_9', 'parameter_value_10', \
                                                  'parameter_value_11', 'parameter_value_12', 'parameter_value_13', 'parameter_value_14', 'parameter_value_15', 'parameter_value_16', 'parameter_value_17', 'parameter_value_18', 'parameter_value_19',
                                                  'parameter_value_20', 'parameter_value_21', 'parameter_value_22', 'parameter_value_23', 'parameter_value_24', 'parameter_value_25', 'parameter_value_26', 'parameter_value_27', 'parameter_value_28', 'parameter_value_29',
                                                  'parameter_value_30', 'parameter_value_31', 'parameter_value_32', 'parameter_value_33', 'parameter_value_34', 'parameter_value_35', 'parameter_value_36', 'parameter_value_37', 'parameter_value_38', 'parameter_value_39',
                                                  'parameter_value_40', 'parameter_value_41', 'parameter_value_42', 'parameter_value_43', 'parameter_value_44', 'parameter_value_45', 'parameter_value_46', 'parameter_value_47', 'parameter_value_48', 'parameter_value_49',
                                                  'parameter_value_50', 'parameter_value_51', 'parameter_value_52', 'parameter_value_53', 'parameter_value_54', 'parameter_value_55', 'parameter_value_56', 'parameter_value_57', 'parameter_value_58', 'parameter_value_59',
                                                  'parameter_value_60', 'parameter_value_61', 'parameter_value_62', 'parameter_value_63', 'parameter_value_64', 'parameter_value_65', 'parameter_value_66', 'parameter_value_67', 'parameter_value_68', 'parameter_value_69',
                                                  'parameter_value_70', 'parameter_value_71', 'parameter_value_72', 'parameter_value_73', 'parameter_value_74', 'parameter_value_75', 'parameter_value_76', 'parameter_value_77', 'parameter_value_78', 'parameter_value_79',
                                                  'parameter_value_80', 'parameter_value_81', 'parameter_value_82', 'parameter_value_83', 'parameter_value_84', 'parameter_value_85', 'parameter_value_86', 'parameter_value_87', 'parameter_value_88', 'parameter_value_89',
                                                  'parameter_value_90',
                                                  'percentage_remaining','amount_remaining','update_time','idpatient']


        df_IOB = pd.DataFrame.from_records(row, columns=labels) # dataframe containing the info fetched from database
        df_df = df_IOB.transpose() # dataframe in column shape
        percentage_remaining = df_df[-4] # double check the slicing
        amount_remaining = df_df[-3]
        df = df_df[1:90]
        IOB_curve = df[~(df == 0).any(axis=1)]

        print('Check that the output is ok')
        return percentage_remaining.values, amount_remaining.values, IOB_curve.values


    ### NO USAGE 2018-07-31
    def get_estimated_Gb_beta_values(self, idestimated_insulin_action):
        """
        Get time variables Gb and ia_factor from database for a given insulin action estimation
        Input: idestimated_insulin_action - ID of insulin action estimation
        Output: Gb and beta values aggregated for all the nights, i.e.,
        from e.g. 00:00 to e.g., 08:00 (time interval TBD) for that specific subject

        """
        print('idestimated_insulin_action: ' + str(idestimated_insulin_action))

        query = text("""SELECT gb, ia_factor, time_stamp, idpatient
                   FROM estimated_gb_ia_factor
                   WHERE idestimated_insulin_action = :p1
                """)
        estimated_GbIAfactor = self.engine.execute(query, {'p1': str(idestimated_insulin_action)})
        row = estimated_GbIAfactor.fetchall()

        labels = ['gb', 'ia_factor', 'time_stamp','idpatient']

        df_aggregated = pd.DataFrame.from_records(row, columns=labels)
        return df_aggregated
    
    def insert_alarm_history(self, df_alarms, idpatient):
        if df_alarms.time_stamp[0][2]=='/':
            format='%m/%d/%Y %H:%M'
        else:
            format='%Y%m%d %H:%M'
        df_alarms.time_stamp = pd.to_datetime(df_alarms.time_stamp, format=format)
        if len(df_alarms) > 0:
            df_alarms['idpatient'] = idpatient
            self.to_sql_insert_ignore(df_alarms, 'alarm_history')
            log.info('SQL INSERT IGNORE alarm_history, idpatient=%s', idpatient)
        else:
            log.info('- SKIPPED SQL INSERT IGNORE alarm_history, idpatient=%s', idpatient)
    

    def insert_bolus_history(self, df_bolus, idpatient, idinsulin, bolustype):
        if len(df_bolus) > 0:
            df_bolus.drop(columns='bolus_type', inplace=True)
            df_bolus = df_bolus.drop_duplicates('time_stamp', keep='last')

            df_bolus.rename(columns={'bolus': 'amount',
                                     'bolus_now': 'amount_now',
                                     'bolus_later': 'amount_later'}, inplace=True)

            df_bolus['idpatient'] = idpatient
            df_bolus['type'] = bolustype
            df_bolus['idinsulin'] = idinsulin

            self.to_sql_insert_ignore(df_bolus, 'bolus_history')
            log.info('SQL INSERT IGNORE bolus_history, idpatient=%s', idpatient)
        else:
            log.info('- SKIPPED SQL INSERT IGNORE bolus_history, idpatient=%s', idpatient)



    def insert_bolus_guide(self, df_bolus_guide, idpatient):
        if len(df_bolus_guide) > 0:
            df_bolus_guide.rename(columns={
                'correction_est': 'guide_correction_dose',
                'food_est': 'guide_meal_dose',
                'iob_est': 'guide_iob',
                'bolus_est': 'guide_bolus'}, inplace=True)

            df_bolus_guide['idpatient'] = idpatient

            self.to_sql_insert_ignore(df_bolus_guide, 'bolus_guide')
            log.info('SQL INSERT IGNORE bolus_guide, idpatient=%s', idpatient)
        else:
            log.info('- SKIPPED SQL INSERT IGNORE bolus_guide, idpatient=%s', idpatient)


    def insert_basal_change_history(self, df_basal_change, idpatient, idinsulin):
        if len(df_basal_change) > 0:
            df_basal_change_history = pd.DataFrame(columns=['time_stamp', 'basal_level', 'idinsulin', 'idpatient'])

            df_basal_change_history['time_stamp'] = df_basal_change['time_stamp']
            df_basal_change_history['basal_level'] = df_basal_change['basal_level']
            df_basal_change_history['idinsulin'] = idinsulin
            df_basal_change_history['idpatient'] = idpatient

            df_basal_change_history = df_basal_change_history.drop_duplicates('time_stamp', keep='last')

            self.to_sql_insert_ignore(df_basal_change_history, 'basal_change_history')
            log.info('SQL INSERT IGNORE basal_change_history, idpatient=%s', idpatient)
        else:
            log.info('- SKIPPED SQL INSERT IGNORE basal_change_history, idpatient=%s', idpatient)



    def insert_sensor(self, start_time=None, model=None, serial_nr=None, idpatient_variables=None,
                      idpatient=None):
        query = text("""INSERT INTO sensor (start_time, model, serial_nr, idpatient_variables, idpatient)
                        VALUES (:p1, :p2, :p3, :p4, :p5)
                     """)
        result = self.engine.execute(query, {'p1': start_time,
                                             'p2': model,
                                             'p3': serial_nr,
                                             'p4': idpatient_variables,
                                             'p5': idpatient})

        log.info('SQL INSERT INTO sensor, model=%s, serial_nr=%s, idpatient_variables=%s)' %
                      (model, serial_nr, idpatient_variables))

        if result.lastrowid > 0:
            return result.lastrowid
        else:
            raise ValueError


    def insert_variable_value(self, time_stamp, value, acronyme, idpatient_variables,
                              idpatient, sample_type, relative_deviation, idsensor):
        """
        Use this function to add BG values, CGM, heartrate etc..
        idpatient_variables, 1 = BG, 2 = CGM, 3 = stresslvl, 4 = heartrate
        """
        if len(time_stamp) > 0:
            df = pd.DataFrame(columns=['time_stamp', 'value', 'acronyme', 'idpatient_variables',
                                       'idpatient', 'sample_type', 'relative_deviation', 'idsensor'])
            df['time_stamp'] = time_stamp
            df['value'] = value
            df['acronyme'] = acronyme
            df['idpatient_variables'] = idpatient_variables
            df['idpatient'] = idpatient
            df['sample_type'] = None
            df['relative_deviation'] = None
            df['idsensor'] = idsensor

            df = df.drop_duplicates('time_stamp', keep='last')

            self.to_sql_insert_ignore(df, 'variable_values')
            log.info('SQL INSERT IGNORE variable_values, idpatient=%s, acronym=%s' % (idpatient, acronyme))
        else:
            log.info('- SKIPPED SQL INSERT IGNORE variable_values, idpatient=%s, acronym=%s' % (idpatient, acronyme))


    def insert_meal_history(self, df_meal, df_recipe_and_ingredients, idpatient):
        """A Meal_history has relation to recipe, ingredient and ingredients_list"""
        if df_meal is not None and len(df_meal) > 0:
            self.insert_ingredients_list(df_recipe_and_ingredients, df_meal, idpatient)
            log.info('SQL INSERT meal_history, ingredients, ingredients_list, recipe')
        else:
            log.info('- SKIPPED SQL INSERT meal_history, ingredients, ingredients_list, recipe')



    def insert_ingredients_list(self, df_recipe_and_ingredients, df_meal, idpatient):
        total_recipe = int(df_recipe_and_ingredients.tail(1)['groupid'])
        for n in range(0, total_recipe + 1):
            group = df_recipe_and_ingredients.loc[df_recipe_and_ingredients['groupid'] == n].copy()
            L2_norm, idrecipe = self.insert_group_into_ingredients_list(group)
            self.insert_meal_to_meal_history(meal_row_df=df_meal.iloc[n], idpatient=idpatient,
                                             L2norm=L2_norm, idrecipe=idrecipe)


    def insert_group_into_ingredients_list(self, group_df):
        group_df = self.merge_all_duplications_of_ingredients(group_df)
        L2_norm = np.linalg.norm(group_df['amount'])
        recipe_name = str(group_df['recipe'].head(1).tolist()[0])
        idrecipe = -1
        is_new_recipe = True

        # normalize amount and change ingredient name to ingredient_id
        for indx, row in group_df.iterrows():
            ingredient = row['ingredient']
            measure_unit = row['measure_unit']
            id_ingredient = self.get_idingredient_with_name(ingredient, measure_unit)
            if id_ingredient == None:
                result = self.insert_ingredient(name=ingredient, measure_unit=measure_unit)
                id_ingredient = result.lastrowid

            group_df.at[indx, 'ingredient'] = id_ingredient

            normalized_amount = group_df['amount'][indx] / L2_norm
            group_df.at[indx, 'amount'] = normalized_amount

        result = self.get_matching_recipes(recipe_name)
        result = result.fetchall()

        if len(result) > 0:

            for x in result:
                current_recipe_id = x[0]
                found_recipe = self.check_if_recipe_ingredient_ratio_matches(current_recipe_id, group_df)
                if found_recipe :
                    idrecipe = current_recipe_id
                    recipe_name = x[1]
                    is_new_recipe = False
                    break
                else :
                    recipe_name = x[1]

        if is_new_recipe :
            result = self.insert_recipe(recipe_name)
            idrecipe = result.lastrowid
            group_df.rename(columns={'recipe': 'idrecipe', 'ingredient': 'idingredient'}, inplace=True)
            group_df.loc[:, ['idrecipe']] = idrecipe
            group_df = group_df.drop({'groupid', 'measure_unit'}, axis=1)

            self.to_sql(group_df, 'ingredients_list', self.engine)
        return L2_norm, idrecipe

    # Todo: Refactor this function into the parser
    def merge_all_duplications_of_ingredients(self, group_df):
        #group_df amount, groupid,   ingredient,           measure_unit, recipe
        #    0       1       0     pasta prima gemelli       1 port          pasta & chicken
        #    0       100     0       kyckling                g               pasta & chicken

        list_ingredient, list_amount, list_groupid, list_measure_unit, list_recipe = [], [], [], [],[]

        for indx, row in group_df.iterrows():
            ingredient = row['ingredient']
            measure = row['measure_unit']
            amount = row['amount']
            recipe = row['recipe']
            groupid = row['groupid']

            if ingredient in list_ingredient :
                merge_index = list_ingredient.index(ingredient)
                if list_measure_unit[merge_index] == measure :
                    new_sum_amount = list_amount[merge_index] + amount
                    list_amount[merge_index] = new_sum_amount
            else :
                list_ingredient.append(ingredient)
                list_amount.append(float(amount))
                list_measure_unit.append(measure)
                list_groupid.append(groupid)
                list_recipe.append(recipe)

        column_data = {'recipe': list_recipe, 'ingredient': list_ingredient, 'amount': list_amount,
                       'measure_unit' : list_measure_unit, 'groupid' : list_groupid }
        group_df = pd.DataFrame(data=column_data)

        return group_df


    def get_matching_recipes(self, recipe_name):
        result = False
        MAX_RECIPE_NAME_LENGTH = 45
        if (len(recipe_name) > MAX_RECIPE_NAME_LENGTH):
            recipe_name = recipe_name[:MAX_RECIPE_NAME_LENGTH]
        if len(recipe_name) > 0 :
            query = text("SELECT idrecipe, name FROM recipe WHERE name=:p1")
            result = self.engine.execute(query, {'p1': recipe_name})

        return result


    def insert_recipe(self, recipe_name):
        MAX_RECIPE_NAME_LENGTH = 45
        if (len(recipe_name) > MAX_RECIPE_NAME_LENGTH):
            recipe_name = recipe_name[:MAX_RECIPE_NAME_LENGTH]
        query = text("insert into recipe (name) values (:p1)")
        result = self.engine.execute(query, {'p1': recipe_name})
        return result


    def get_ingredients_list_with_idrecipe(self, recipe_id):
        query = text("select * from ingredients_list where idrecipe=':p1'")
        result = self.engine.execute(query, {'p1': recipe_id})
        return result


    def check_if_recipe_ingredient_ratio_matches(self, recipe_id, ingredients_df):
        result = self.get_ingredients_list_with_idrecipe(recipe_id)
        matching_ing_list = self.convert_sql_result_to_df(result)
        return self.check_if_ingredients_are_equal(matching_ing_list, ingredients_df)


    def check_if_ingredients_are_equal(self, db_ingredients_group, recipe_ingredients_group_df):
        ingredient_amount_matches = True
        size1 = len(db_ingredients_group)
        size2 = len(recipe_ingredients_group_df)

        if size1 == size2 :
            for indx, row in db_ingredients_group.iterrows() :
                db_recipe_ingredientid =  row['idingredient']
                db_recipe_amount = row['amount']

                matching_ingredient = recipe_ingredients_group_df.loc[recipe_ingredients_group_df['ingredient'] == db_recipe_ingredientid]
                new_recipe_ingredient_amount = matching_ingredient.loc[:,'amount'].values
                if len(matching_ingredient) > 0 :
                    #break function as soon the recipe ratio mismatch.
                    if len(new_recipe_ingredient_amount) > 0 :
                        new_recipe_ingredient_amount = new_recipe_ingredient_amount[0]
                    if self.ingredient_amount_is_a_mismatch(new_recipe_ingredient_amount, db_recipe_amount):
                        ingredient_amount_matches = False
                        break
                else :
                    ingredient_amount_matches = False
                    break
        else:
            ingredient_amount_matches = False

        return ingredient_amount_matches


    def ingredient_amount_is_a_mismatch(self, amount1, amount2):
        try:
            np.testing.assert_almost_equal(amount1, amount2, decimal=6)
            return False
        except AssertionError as er :
            return True
        except TypeError as er :
            raise er


    def insert_meal_to_meal_history(self, meal_row_df, idpatient, L2norm, idrecipe):
        df = meal_row_df.drop(['ingredients', 'recipe'], axis=0)
        df = df.to_frame().T

        df.loc[:, 'idpatient'] = idpatient
        df.loc[:, 'idrecipe'] = idrecipe
        df.loc[:, 'portion_size'] = L2norm
        df.loc[:, 'fat'] = df.loc[:, 'fat']
        df.loc[:, 'carbohydrate'] = df.loc[:, 'carbohydrate']
        df.loc[:, 'protein'] = df.loc[:, 'protein']
        df.loc[:, 'calories'] = df.loc[:, 'calories']
        df.loc[:, 'alcohol'] = df.loc[:, 'alcohol']
        df.loc[:, 'time_stamp'] = pd.to_datetime(df['time_stamp'])
        #df['source'] = 'diasend'

        self.to_sql_insert_ignore(df, 'meal_history')

    def insert_ingredient(self, name, measure_unit):
        name_with_measureunit = name + ' (%s)' % measure_unit
        query = text("insert into ingredient (name) values (:p1)")
        result = self.engine.execute(query, {'p1': name_with_measureunit})
        return result


    def get_idingredient_with_name(self, name, measure_unit):
        name_with_measureunit = name + ' (%s)' % measure_unit
        query = text("select idingredient from ingredient where name = :p1")
        result = self.engine.execute(query, {'p1': name_with_measureunit})
        result = result.fetchone()
        if result is not None:
            idingredient = result[0]
            return idingredient
        else:
            return None


    def get_df_meal_history(self, idpatient):
        query = "select * from meal_history where idpatient=%s" % idpatient
        df = self.read_sql_query(query)
        return df


    def get_df_all_meal_history_with_recipe_name(self, idpatient):
        query = """SELECT meal_history.*, recipe.name
                   FROM meal_history
                   INNER JOIN recipe ON meal_history.idrecipe = recipe.idrecipe
                   WHERE meal_history.idpatient=%s
                """ % idpatient
        df = self.read_sql_query(query)
        return df


    def get_bolus_history_for_meal_start_time(self, meal_start_time, next_meal_start_time, idpatient):
        # Input: the timestamp for a meal instance and the next meal instance in turn 
        # Returns: List of boluses that should be associated to this meal and the duration since the first timestamp 
        list_of_idbolus = []
        list_delta_minutes = []
        # We associate boluses just before the meal as well as any corrections done during the post-prandial phase 
        search_window_start = meal_start_time - timedelta(minutes=40)
        search_window_end = meal_start_time + timedelta(hours=3)

        if next_meal_start_time is not None:
            # check if next meal is too close, and if so only tag bolues before the meal
            if(next_meal_start_time - timedelta(minutes=30) <= meal_start_time):
                search_window_end = meal_start_time
            else:
                search_window_end = min(search_window_end, next_meal_start_time - timedelta(minutes=31))

        query = text("""SELECT idbolus_history, time_stamp
                        FROM bolus_history
                        WHERE idpatient=:p1 AND
                             time_stamp >= :p2 AND
                             time_stamp <= :p3 AND
                             amount > 0
                     """)
        result = self.engine.execute(query, {'p1': idpatient,
                                             'p2': str(search_window_start),
                                             'p3': str(search_window_end)})

        if result.rowcount > 0:
            result = result.fetchall()
            for idbolus in result:
                list_of_idbolus.append(idbolus[0])
                bolus_timestamp = idbolus[1]
                delta_time_seconds = (bolus_timestamp - meal_start_time).total_seconds()
                delta_minutes = delta_time_seconds / 60
                list_delta_minutes.append(delta_minutes)

        return list_of_idbolus, list_delta_minutes
    
    def get_mean_time_stamp_for_meal_type(self, idpatient, meal_type):
        query = text("""SELECT AVG(HOUR(time_stamp)) FROM meal_history WHERE idpatient=:p1 AND meal_type=:p2""")
        result = self.engine.execute(query, {'p1': idpatient, 'p2': meal_type})
        return result.fetchone()[0]
    
    def get_mean_carbs_for_meal_type(self, idpatient, meal_type):
        query = text("""SELECT AVG(carbohydrate) FROM meal_history WHERE idpatient=:p1 AND meal_type=:p2""")
        result = self.engine.execute(query, {'p1': idpatient, 'p2': meal_type})
        return result.fetchone()[0]
                                        
    def update_bolus_with_idmeal_and_delta_minutes(self, bolus_ids, idmeal, delta_minutes):
        # Input: List of bolus doses, a meal instance id, and a vector of the duration in-between the meal start and the bolus doses 
        # No return value, instead the bolus history is updated with a meal tagging
        if (bolus_ids is not None) and (len(bolus_ids) > 0):
            # for bolusid in bolus_ids:
            for indx in range(0, len(bolus_ids)):
                query = text("""UPDATE bolus_history
                                SET idmeal_history=:p1, delta_minutes_from_meal=:p2
                                WHERE idbolus_history=:p3
                             """)
                result = self.engine.execute(query, {'p1': idmeal, 'p2': delta_minutes[indx], 'p3': bolus_ids[indx]})


    def get_all_cgm(self, idpatient, time_of_day, end_date='2000-01-01'):
        
        # Protection against data overflow
        if end_date=='2000-01-01':
            # get latest date
            end_date = self.get_timestamp_last_insert_patient_variables(idpatient, 2) 
            if end_date:
                end_date += timedelta(days=-90)
            else:
                return None
        if not isinstance(end_date,str):
            end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")
        
        # Retrieves CGM data for a specific day time period
        day_times_start = {'all': '00:00:00',
                           'morning': '06:00:00',
                           'afternoon': '12:00:00',
                           'evening': '18:00:00',
                           'night': '00:00:00'}

        day_times_end = {'all': '23:59:59',
                         'morning': '11:59:59',
                         'afternoon': '17:59:59',
                         'evening': '23:59:59',
                         'night': '05:59:59'}

        query = """SELECT time_stamp, value, acronyme, idpatient
                   FROM variable_values
                   WHERE acronyme='CGM' AND
                   idpatient=%s AND
                   TIME(time_stamp) BETWEEN '%s' AND '%s' AND time_stamp> '%s'
                """ % (idpatient, day_times_start[time_of_day], day_times_end[time_of_day], end_date)

        df_cgm = self.read_sql_query(query)
        df_cgm = df_cgm.set_index(pd.DatetimeIndex(df_cgm['time_stamp']))
        df_cgm = df_cgm.drop(['time_stamp'], axis=1)
        return df_cgm


    def insert_variable_values_stats(self, df):
        if len(df) > 0:
            self.to_sql_insert_ignore(df, 'variable_values')
            idpatient = df['idpatient'].iloc[0]
            for acronyme in df['acronyme'].unique():
                log.info(('Added ' + acronyme + ' for idpatient %s') %(idpatient))


    def insert_cluster_pattern_cache(self, df, cluster_name, period):
        query = text("SELECT idcluster_pattern FROM cluster_pattern WHERE name = :p1")
        result = self.engine.execute(query, {'p1': cluster_name})

        if result.rowcount > 0:
            cluster_id = result.fetchone()[0]
            df['idcluster_pattern'] = cluster_id
            df['period'] = period
            self.to_sql(df,'cluster_pattern_cache', self.engine)
        else:
            log.error('CANNOT find idcluster_pattern in DB')


    def insert_cluster_pattern_day(self, df, cluster_name, period):
        query = text("SELECT idcluster_pattern FROM cluster_pattern WHERE name = :p1")
        result = self.engine.execute(query, {'p1': cluster_name})

        if result.rowcount > 0:
            cluster_id = result.fetchone()[0]
            df['idcluster_pattern'] = cluster_id
            df['period'] = period
            self.to_sql(df,'cluster_pattern_day', self.engine)
        else:
            log.error('CANNOT find idcluster_pattern in DB')


    def insert_cluster_pattern(self, name):
        query = text('INSERT INTO cluster_pattern (name) VALUES (:p1)')
        result = self.engine.execute(query, {'p1': name})
        return result


    def delete_cluster_pattern_cache_for_patient(self, idpatient):
        query = text("DELETE from cluster_pattern_cache where idpatient = :p1")

        result = self.engine.execute(query, {'p1': idpatient})
        log.info("SQL DELETE from cluster_pattern_cache, idpatient=%s" % idpatient)
        return result


    def delete_cluster_pattern_day_for_patient(self, idpatient):
        query = text("DELETE from cluster_pattern_day where idpatient = :p1")

        result = self.engine.execute(query, {'p1': idpatient})
        log.info("SQL DELETE from cluster_pattern_day, idpatient=%s" % idpatient)
        return result


    def get_df_cluster_pattern_day(self, idpatient):
        query = "select * from cluster_pattern_day where idpatient=%s" % idpatient
        df = self.read_sql_query(query)
        return df


    def get_middle_cgm_between_timestamp(self, start_time, end_time, idpatient, idpatient_variables):
        cgm_value = None
        query = text("""SELECT value
                        FROM variable_values
                        WHERE idpatient=:p1 AND
                              idpatient_variables=:p2 AND
                              time_stamp between :p3 AND :p4
                     """)
        result = self.engine.execute(query, {'p1': idpatient,
                                             'p2': idpatient_variables,
                                             'p3': str(start_time),
                                             'p4': str(end_time)})

        if result.rowcount > 0:
            middle = result.rowcount // 2
            cgm_value = result.fetchall()[middle][0]
        return cgm_value


    def get_bolus_history_all_columns(self, idpatient, end_date='2000-01-01'):
        # Protection against overflow - default to no more than 3 months
        if end_date=='2000-01-01':
            # get latest date
            end_date = self.get_latest_timestamp(idpatient, 'bolus_history')
            if end_date:
                end_date += timedelta(days=-90)
            else:
                return None
            
        if not isinstance(end_date,str):
            end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")
        
        query = """SELECT * FROM bolus_history WHERE idpatient = %s AND time_stamp> '%s'""" % (idpatient, end_date)
        bolus_df = self.read_sql_query(query)
        return bolus_df


    def get_df_bolus_history_between_timestamp(self, idpatient, start_time, end_time):
        """BETWEEN IS INCLUSIVE, meaning starttime >= time <= endtime"""

        query = """SELECT *
                   FROM bolus_history
                   WHERE idpatient=%s AND
                   time_stamp BETWEEN '%s' AND '%s'
                """ % (idpatient, start_time, end_time)
        bolus_df = self.read_sql_query(query)
        return bolus_df


    def get_tagged_meal_bolus_all_columns(self, idpatient):
        query = 'select * from bolus_history where idpatient=%s and idmeal_history is not null' % idpatient
        bolus_df = self.read_sql_query(query)
        return bolus_df
    
    def get_fast_correction_bolus(self, idpatient):
        query = """SELECT * FROM bolus_history WHERE idpatient=%s AND idinsulin IN 
                (SELECT idinsulin FROM insulin WHERE type in ('rapid-acting','ultra rapid-acting')) 
                AND idmeal_history IS NULL""" % idpatient
        bolus_df = self.read_sql_query(query)
        return bolus_df

    def get_bolus_guide_of_meal_type(self, idpatient, meal_type, start_date, end_date):
        query = """SELECT bg.*
                   FROM bolus_guide bg JOIN meal_history mh ON bg.idmeal_history=mh.idmeal_history
                   WHERE bg.idpatient=%s AND mh.meal_type='%s' AND 
                   bg.time_stamp BETWEEN '%s' AND '%s'
                """ % (idpatient, meal_type, start_date, end_date)
        bolus_df = self.read_sql_query(query)
        return bolus_df
        
    def get_cgm_values_between_timestamp(self, start_time, end_time, idpatient):

        patient_variables = 2  #2 = CGM
        query = """SELECT value, time_stamp
                        FROM variable_values
                        WHERE idpatient=%s AND
                              idpatient_variables=%s AND
                              time_stamp between '%s' AND '%s'
                     """ % (idpatient, patient_variables, str(start_time), str(end_time))

        df_cgm = self.read_sql_query(query)
        return df_cgm


    def get_start_time_for_next_meal(self, idmeal_history, idpatient):
        start_time_next_meal = None
        query = text("""SELECT time_stamp
                        FROM meal_history
                        WHERE idmeal_history > :p1 AND idpatient=:p2 limit 1
                     """)
        result = self.engine.execute(query, {'p1': idmeal_history, 'p2': idpatient})

        if result.rowcount > 0:
            start_time_next_meal = result.fetchone()[0]

        return start_time_next_meal


    def get_timestamp_from_latest_meal_history(self, idpatient):
        timestamp = None
        query = text("""SELECT time_stamp
                        FROM meal_history
                        WHERE idpatient=:p1
                        ORDER BY idmeal_history DESC LIMIT 0,1
                     """)
        result = self.engine.execute(query, {'p1': idpatient})
        if result.rowcount > 0 :
            timestamp = result.fetchone()[0]
        return timestamp


    def get_timestamp_from_first_meal_history(self, idpatient):
        timestamp = None
        query = text("""SELECT time_stamp
                        FROM meal_history
                        WHERE idpatient=:p1
                        ORDER BY idmeal_history
                        ASC LIMIT 0,1
                     """)
        result = self.engine.execute(query, {'p1': idpatient})
        if result.rowcount > 0 :
            timestamp = result.fetchone()[0]
        return timestamp


    def get_last_insert_cache_meal_glucose_mean(self):
        id = None
        query = "SELECT MAX(idcache_meal_glucose_mean) FROM cache_meal_glucose_mean"
        result = self.engine.execute(query)
        if result.rowcount > 0 :
            id = result.fetchone()[0]
        return id


    def delete_cache_meal_glucose_mean_for_patient(self, idpatient):
        query = text("""DELETE FROM cache_meal_glucose_mean where idpatient = :p1""")
        result = self.engine.execute(query, {'p1': idpatient})
        log.info("SQL DELETE from cache_meal_glucose_mean, idpatient=%s" % idpatient)
        return result


    def delete_cache_stats_basal(self, idpatient):
        self.delete_cache_stats_for_patient(idpatient, 'basal')


    def delete_cache_stats_glucose(self, idpatient):
        self.delete_cache_stats_for_patient(idpatient, 'glucose')


    def delete_cache_stats_for_patient(self, idpatient, data_type):
        query = text("DELETE from cache_stats where idpatient = :p1 and type = :p2")

        result = self.engine.execute(query, {'p1': idpatient, 'p2': data_type})
        log.info("SQL DELETE from cache_stats idpatient=%s type=%s" % (idpatient, data_type))
        return result



    def insert_cache_stats(self, time_period, type, idpatient, mean, std, min, max):
        query = text("""insert into cache_stats (time_period, type, idpatient, value_mean,
                                                 value_mean_std, value_min, value_max)
                        values (:p1, :p2, :p3, :p4, :p5, :p6, :p7)
                     """)
        result = self.engine.execute(query, {'p1': time_period,
                                             'p2': type,
                                             'p3': idpatient,
                                             'p4': self.encode_float64(mean),
                                             'p5': self.encode_float64(std),
                                             'p6': self.encode_float64(min),
                                             'p7': self.encode_float64(max)})
        return result


    def get_df_all_patient_values_for_period(self, start_date, end_date, idpatient, patient_variables):
        query = text("""SELECT time_stamp, value, idpatient_variables
                        FROM variable_values
                        WHERE idpatient=:p1
                        AND idpatient_variables=:p2
                        AND time_stamp >= :p3 AND time_stamp <= :p4
                """)

        result = self.engine.execute(query, {'p1': idpatient,
                                             'p2': patient_variables,
                                             'p3': start_date.date(),
                                             'p4': end_date.date()})
        result = result.fetchall()

        labels = ['time_stamp', 'value', 'idpatient_variables']
        df_cgm_values = pd.DataFrame.from_records(result, columns=labels)
        return df_cgm_values
    
    def get_patient_variable_data_after(self, idpatient, acronym, time_start):
        query = """SELECT time_stamp, value
                        FROM variable_values
                   WHERE idpatient=%s AND acronyme='%s' AND time_stamp >='%s' 
                """ % (idpatient, acronym, time_start)
        return self.read_sql_query(query)


    def get_df_latest_all_variable_values(self, idpatient):
        query = text("select distinct(idpatient_variables) from variable_values where idpatient=:p1")
        result = self.engine.execute(query, {'p1': idpatient})   
        result = result.fetchall()
        variable_list = [r[0] for r in result]
        
        labels = ['time_stamp', 'value', 'idpatient_variables', 'acronyme']
        df_latest_values = pd.DataFrame(columns=labels)
        for idvariable in variable_list:
            query = """SELECT  time_stamp, value, idpatient_variables, acronyme 
                     FROM variable_values 
                    WHERE idpatient=%s AND idpatient_variables=%s 
                    ORDER BY time_stamp DESC LIMIT 1"""%(idpatient, idvariable)
            df1 = pd.read_sql(query, self.engine)
            df_latest_values = df_latest_values.append(df1)
        
        return df_latest_values        

    def get_latest_variable_value(self, idpatient, acronyme):
        query = text("""SELECT value
                        FROM variable_values
                        WHERE idpatient=:p1
                        AND acronyme=:p2 ORDER BY time_stamp DESC LIMIT 1
                     """)
        result = self.engine.execute(query, {'p1': idpatient, 'p2': acronyme})
        if result.rowcount > 0:
            result = result.fetchone()[0]  
        else: 
            result=np.nan 
        return result

    def update_variable_values_latest(self, idpatient):
        df = self.get_df_latest_all_variable_values(idpatient)

        if not df.empty:
            df['idpatient'] = idpatient
            query = text("""DELETE FROM variable_values_latest
                        WHERE idpatient=:p1
                        """)
            self.engine.execute(query, {'p1': idpatient})

            self.to_sql(df, 'variable_values_latest', self.engine)

            log.info('SQL UPDATE variable_values_latest, idpatient=%s' % (idpatient))


    def get_idpatient_variables(self, acronyme):
        idpatient_variables = None
        query = text("""SELECT idpatient_variables
                        FROM patient_variables
                        WHERE acronyme=:p1
                     """)
        result = self.engine.execute(query, {'p1': acronyme})
        if result.rowcount > 0:
            idpatient_variables = result.fetchone()[0]  
        return idpatient_variables

    def get_df_all_variable_limits(self, idclinic):
        query = text("""SELECT alert_low, abnormal_low, lower_limit_normal, 
                        upper_limit_normal, abnormal_high, alert_high, 
                        idpatient_variables, gender, age_lower, age_upper
                        FROM limits_general
                        WHERE idclinic=:p1
                     """)
        result = self.engine.execute(query, {'p1': idclinic})
        result = result.fetchall()
        labels = ['alert_low', 'abnormal_low', 'lower_limit_normal', 
                    'upper_limit_normal', 'abnormal_high', 'alert_high',
                    'idpatient_variables', 'gender', 'age_lower', 'age_upper']
        df_limits = pd.DataFrame.from_records(result, columns=labels)
        return df_limits

    def get_limits_general_for_variable(self, gender, idclinic, age, idpatient_variables):
        query = text("""SELECT alert_low, abnormal_low, lower_limit_normal, 
                        upper_limit_normal, abnormal_high, alert_high
                        FROM limits_general
                        WHERE gender in (:p1, 'all')
                        AND idclinic=:p2
                        AND age_lower<=:p3
                        AND age_upper>=:p3
                        AND idpatient_variables=:p4
                     """)
        result = self.engine.execute(query, {'p1': gender, 'p2':idclinic, 'p3':age, 'p4': idpatient_variables})
        if result.rowcount > 0:
            result = [dict(row) for row in result]
            result = result[0]
        else: 
            result = {'alert_low':np.nan, 'abnormal_low':np.nan, 'lower_limit_normal':np.nan, 
                        'upper_limit_normal':np.nan, 'abnormal_high':np.nan, 'alert_high':np.nan} 
        return result        
    
    def get_cache_meal_glucose_mean_updated_at(self, idpatient):
        query = text("select max(updated_at) from cache_meal_glucose_mean where idpatient= :p1")
        result = self.engine.execute(query, {'p1': idpatient})
        timestamp = result.fetchone()[0]
        return timestamp
    
    def get_insights_updated_at(self, idpatient):
        query = text("select max(created_at) from cache_patient_messages where idpatient= :p1")
        result = self.engine.execute(query, {'p1': idpatient})
        timestamp = result.fetchone()[0]
        return timestamp
        
    def get_latest_basal_history(self, idpatient):
        query = text("select max(time_stamp) from basal_change_history where idpatient= :p1")
        result = self.engine.execute(query, {'p1': idpatient})
        timestamp = result.fetchone()[0]
        return timestamp
    
    def get_latest_meal_history(self, idpatient):
        query = text("select max(time_stamp) from meal_history where idpatient= :p1")
        result = self.engine.execute(query, {'p1': idpatient})
        timestamp = result.fetchone()[0]
        return timestamp
        
    def get_timestamp_last_insert_patient_variables(self, idpatient, patient_variables):
        query = text("""SELECT max(time_stamp)
                        FROM variable_values
                        WHERE idpatient=:p1
                        AND idpatient_variables=:p2
                     """)
        result = self.engine.execute(query, {'p1': idpatient, 'p2': patient_variables})
        timestamp = result.fetchone()[0]
        return timestamp
    
    def get_timestamp_last_insert_acronym(self, idpatient, acronym):
        query = text("""SELECT max(time_stamp)
                        FROM variable_values
                        WHERE idpatient=:p1
                        AND acronyme=:p2
                     """)
        result = self.engine.execute(query, {'p1': idpatient, 'p2': acronym})
        timestamp = result.fetchone()[0]
        return timestamp
    
    def get_latest_bolus_time_stamp(self, idpatient):
        query = text("select max(time_stamp) from bolus_history where idpatient= :p1")
        result = self.engine.execute(query, {'p1': idpatient})
        timestamp = result.fetchone()[0]
        return timestamp
    
    def write_latest_upload_date(self, idpatient):
        # CGM
        latest_upload_time = self.get_timestamp_last_insert_patient_variables(idpatient, 2) or " never "
        # BG
        if latest_upload_time==" never ":
            latest_upload_time = self.get_timestamp_last_insert_patient_variables(idpatient, 1) or " never "
        # bolus_history
        if latest_upload_time==" never ":
            latest_upload_time = self.get_latest_bolus_time_stamp(idpatient) or " never "
        
        query = text("""UPDATE IGNORE patient SET latest_upload_date = :p1 
                    WHERE idpatient = :p2
                    """)
        log.info(latest_upload_time)
        self.engine.execute(query, {'p1': latest_upload_time, 'p2': idpatient})

    def get_latest_upload_date(self, idpatient):
        query = text("""SELECT latest_upload_date FROM patient
                    WHERE idpatient = :p1
                    """)
        result = self.engine.execute(query, {'p1': idpatient})
        timestamp = result.fetchone()[0] or " never "
        return timestamp

    def get_latest_basal_programs(self, idpatient):
        query = """SELECT idtherapy_settings_value, idtherapy_settings, 
                    profile, cast(start_time as char) as start_time,
                    value, value_type FROM therapy_settings_value WHERE
                    idtherapy_settings = (SELECT max(idtherapy_settings) 
                    FROM therapy_settings WHERE idpatient= %s AND therapy_settings_type='current')
                    AND value_type='basal_program'
                    """ %idpatient
        return self.read_sql_query(query)
    
    def get_basal_analysis_end_date(self, idpatient):
        query = text("""SELECT max(end_date_entry) from therapy_settings ts join therapy_settings_value tsv on ts.idtherapy_settings=tsv.idtherapy_settings 
where ts.idpatient= :p1 and therapy_settings_type='analysis' and tsv.value_type='basal_program'
                     """)
        result = self.engine.execute(query, {'p1': idpatient})
        timestamp = result.fetchone()[0]
        return timestamp

    def get_latest_analysis_basal_program(self, idpatient):
        query = """SELECT start_time, value FROM therapy_settings_value WHERE
                    idtherapy_settings = (SELECT max(idtherapy_settings) 
                    FROM therapy_settings WHERE idpatient= %s AND therapy_settings_type='analysis')
                    AND value_type='basal_program'
                    """ %idpatient
        return self.read_sql_query(query)
    
    def get_df_latest_basal_program_between_dates(self, idpatient, start_date, end_date):
        query = text("""SELECT 60*HOUR(tsv.start_time) + MINUTE(tsv.start_time) as time, 
                     tsv.value FROM
                     (SELECT * FROM therapy_settings 
                      WHERE therapy_settings_type='analysis' AND 
                      idpatient=:p1 AND end_date_entry BETWEEN :p2 AND :p3 
                      ORDER BY idtherapy_settings DESC LIMIT 1) ts 
                     JOIN therapy_settings_value tsv
                     ON ts.idtherapy_settings=tsv.idtherapy_settings
                     WHERE tsv.idpatient=:p1 AND tsv.value_type='basal_program'""")
                     
        return pd.read_sql(query, self.engine, params={'p1': idpatient, 
                                           'p2': start_date, 
                                           'p3': end_date})
    
    def get_df_latest_therapy_setting_profile_between_dates(self, 
                                                         idpatient, 
                                                         start_date, 
                                                         end_date,
                                                         settings_type,
                                                         TS_type,
                                                         profile):
        query = text("""SELECT 60*HOUR(tsv.start_time) + MINUTE(tsv.start_time) as time, 
                     tsv.value FROM
                     (SELECT * FROM therapy_settings 
                      WHERE therapy_settings_type=:p5 AND 
                      idpatient=:p1 AND end_date_entry BETWEEN :p2 AND :p3 
                      ORDER BY idtherapy_settings DESC LIMIT 1) ts 
                     JOIN therapy_settings_value tsv
                     ON ts.idtherapy_settings=tsv.idtherapy_settings
                     WHERE tsv.idpatient=:p1 AND tsv.value_type=:p4 AND profile=:p6""")
                     
        return pd.read_sql(query, self.engine, params={'p1': idpatient, 
                                           'p2': start_date, 
                                           'p3': end_date,
                                           'p4': settings_type,
                                           'p5': TS_type,
                                           'p6': profile})

    def is_therapy_settings_new(self, idpatient, start_date_entry, end_date_entry, therapy_settings_type, description, value_type = 'basal_program'):
        """Check that we do not have the same therapy settings in DB already. 
        Criteria is that tha analysis has been issued for a dataset no more than a week apart from the new one."""
        query = text("""SELECT ts.idtherapy_settings from therapy_settings_value tsv JOIN therapy_settings ts ON tsv.idtherapy_settings=ts.idtherapy_settings
                        WHERE ts.idpatient = :p1 AND
                        (ABS(TIMESTAMPDIFF(DAY, start_date_entry,:p2))<7 AND
                        ABS(TIMESTAMPDIFF(DAY, end_date_entry,:p3))<7)
                        AND ts.therapy_settings_type = :p4 AND 
                        ts.description = :p5 AND 
                        tsv.value_type = :p6
                        ORDER BY tsv.updated_at DESC
                     """)
        result = self.engine.execute(query, {'p1': idpatient,
                                             'p2': str(start_date_entry),
                                             'p3': str(end_date_entry),
                                             'p4': therapy_settings_type,
                                             'p5': description,
                                             'p6': value_type})

        if result.rowcount > 0:
            return False, result.fetchone()[0]
        else:
            return True, None


    def insert_therapy_settings(self, idpatient, serial_nr, brand, model, iob_enabled, iob_duration, 
                                df_pump_cir, df_pump_isf, current_basal_program, df_basal_prog, df_glucose_target, 
                                start_date_entry, end_date_entry, description = 'Default', active = 1):
        
        log.info('Starting inserting ts')
        
        df_profile_pen_dosing_meal_bolus = None
        df_profile_pen_dosing_basal = None

        idpump = self.insert_pump(idpatient, serial_nr, brand, model)

        if idpump == 'NULL':
            treatment_type = None
        else:
            treatment_type = 'pump'

        if active: # Only one current therapy setting may be active
            query = text("""SELECT idtherapy_settings FROM therapy_settings WHERE idpatient = :p1 AND 
therapy_settings_type = :p2 AND active = 1""")
            result = self.engine.execute(query, {'p1': idpatient,
                                            'p2': 'current'})
            log.info('result: {}'.format(result))
            if result.rowcount > 0:
                idts = result.fetchone()[0]
                query = text("""UPDATE therapy_settings SET active = 0 WHERE idtherapy_settings = :p1 """)
                self.engine.execute(query, {'p1': idts})

        # insert into therapy settings
        query = text("""INSERT into therapy_settings (idpatient, therapy_settings_type,
                                                        treatment_type, iob_duration, iob_enabled,
                                                        description, start_date_entry,
                                                        end_date_entry, active)
                        values (:p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9)
                        """)
        result = self.engine.execute(query, {'p1': idpatient, 'p2': 'current', 'p3': treatment_type,
                                    'p4': iob_duration, 'p5': iob_enabled, 'p6': description,
                                    'p7': start_date_entry, 'p8': end_date_entry,
                                    'p9': active})
        idtherapy_settings = result.lastrowid
        log.info('TS inserted')
        # Pen dosing not used
        idpen_meal_bolus = self.insert_profile_pen_dosing(df_profile_pen_dosing_meal_bolus)
        idpen_basal = self.insert_profile_pen_dosing(df_profile_pen_dosing_basal)
        if not df_basal_prog.empty:
            self.insert_basal_program_for_pump(idpatient, df_basal_prog, idtherapy_settings)
        if not df_pump_isf.empty:
            self.insert_profile_isf(idpatient, df_pump_isf, idtherapy_settings)
        if not df_pump_cir.empty:
            self.insert_profile_cir(idpatient, df_pump_cir, idtherapy_settings)
        if not df_glucose_target.empty:
            self.insert_profile_glucose_target(idpatient, df_glucose_target, idtherapy_settings)
            
        
        log.info("SQL INSERT into therapy_settings complete, idpatient=%s" % idpatient)

    def insert_therapy_settings_analysis(self, idpatient, df_basal_prog,
                                         start_date_entry, end_date_entry):

        #is_this_therapy_new, idtherapy_settings = self.is_therapy_settings_new(idpatient, start_date_entry, end_date_entry, 'analysis', description='Default')
        
        #if is_this_therapy_new:
        df_profile_pen_dosing_meal_bolus = None
        df_profile_pen_dosing_basal = None

        treatment_type = None
        iob_duration = None
        iob_enabled = None
        description = None
        # insert into therapy settings
        active = 1
        description = 'Default'
        query = text("""INSERT into therapy_settings (idpatient, therapy_settings_type,
                                                        treatment_type, iob_duration, iob_enabled,
                                                        description, start_date_entry,
                                                        end_date_entry, active)
                        values (:p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9)
                        """)
        result = self.engine.execute(query, {'p1': idpatient, 'p2': 'analysis', 'p3': treatment_type,
                                    'p4': iob_duration, 'p5': iob_enabled, 'p6': description,
                                    'p7': start_date_entry, 'p8': end_date_entry,
                                    'p9': active})
        idtherapy_settings = result.lastrowid

        self.insert_basal_program_for_pump(idpatient, df_basal_prog, idtherapy_settings)

        log.info("SQL INSERT into therapy_settings complete, idpatient=%s" % idpatient)
        #else:
        #    log.info("SKIPPED SQL INSERT of basal profile into analysis therapy_settings, idpatient=%s" % idpatient)


    def insert_pump(self, idpatient, serial_nr, brand, model):
        
        #if serial_nr is not None: 
        # Check if the pump is already registered
        query = text("""SELECT idpump FROM pump WHERE idpatient = :p1 and serial_nr = :p2""")
        result = self.engine.execute(query, {'p1': idpatient, 'p2': serial_nr})
        if result.rowcount > 0:
            log.info('Found pump')
            idpump = result.lastrowid
        else: # Ok, let's insert then      
            query = text("""INSERT into pump (idpatient, serial_nr, brand, model)
                    VALUES (:p1, :p2, :p3, :p4)""")
            result = self.engine.execute(query, {'p1': idpatient, 'p2': serial_nr, 'p3': brand, 'p4': model})
            log.info('SQL INSERT INTO PUMP, idpatient=%s, brand=%s, model=%s' % (idpatient,  brand, model))
            if result.rowcount > 0:
                idpump = result.lastrowid
                log.info('Pump inserted')
            else:
                idpump = None
                log('Failed to insert pump')
        return idpump

    def insert_basal_program_for_pump(self, idpatient, df_basal_prog, idtherapy_settings):
        if idpatient == 'NULL':
            raise ValueError('idpatient=NULL')
        df_temp = df_basal_prog.rename(index=str, columns={"program": "profile", 'basal_level': 'value'})

        df_temp['value_type'] = 'basal_program'
        df_temp['idpatient'] = idpatient
        df_temp['idtherapy_settings'] = idtherapy_settings
        self.to_sql(df_temp, 'therapy_settings_value', self.engine)
        log.info('SQL INSERT into therapy_settings_value, value_type=basal_program, idpatient=%s' % idpatient)


    def insert_profile_pen_dosing(self, df_profile_pen_dosing):
        """idpatient, idinsulin, profile, start_time, dose"""
        return None


    def insert_profile_isf(self, idpatient, df_pump_isf, idtherapy_settings):
        """idpatient, profile, start_time, isf"""
        if len(df_pump_isf) < 1:
            return None
        df_pump_isf.rename(index=str, columns={'isf': 'value'}, inplace=True)

        df_pump_isf['value_type'] = 'isf'
        df_pump_isf['idpatient'] = idpatient
        df_pump_isf['idtherapy_settings'] = idtherapy_settings
        self.to_sql(df_pump_isf, 'therapy_settings_value', self.engine)
        log.info('SQL INSERT into therapy_settings_value, value_type=isf, idpatient=%s' % idpatient)


    def insert_profile_cir(self, idpatient, df_pump_cir, idtherapy_settings):
        """idpatient, profile, start_time, cir"""
        if len(df_pump_cir) < 1:
            return None
        df_pump_cir.rename(index=str, columns={'cir': 'value'}, inplace=True)

        df_pump_cir['value_type'] = 'cir'
        df_pump_cir['idpatient'] = idpatient
        df_pump_cir['idtherapy_settings'] = idtherapy_settings
        self.to_sql(df_pump_cir, 'therapy_settings_value', self.engine)
        log.info(
            'SQL INSERT into therapy_settings_value, value_type=cir, idpatient=%s' % idpatient)


    def insert_profile_glucose_target(self, idpatient, df_glucose_target, idtherapy_settings):
        """idpatient, profile, start_time, glucose_target, glucose_target_std"""
        if len(df_glucose_target) < 1:
            return None
        df_glucose_target.rename(index=str, columns={'glucose_target': 'value'}, inplace=True)
        df_glucose_target.drop(columns='glucose_target_std', inplace=True, errors = 'ignore')

        df_glucose_target['value_type'] = 'glucose_target'
        df_glucose_target['idpatient'] = idpatient
        df_glucose_target['idtherapy_settings'] = idtherapy_settings
        self.to_sql(df_glucose_target, 'therapy_settings_value', self.engine)
        log.info('SQL INSERT into therapy_settings_value, value_type=glucose_target, idpatient=%s' % idpatient)


    def get_basal_within_time_frame(self, idpatient, time_start, time_end):
        query = """SELECT * from basal_change_history
                   WHERE idpatient=%s AND
                   time_stamp >= '%s' AND
                   time_stamp <= '%s'
                """ % (idpatient, time_start, time_end)
        return self.read_sql_query(query)
    
    
    def get_all_basal_history(self, idpatient):
        query = """SELECT time_stamp AS time_stamp_basal, basal_level AS basal, idpatient
                   FROM basal_change_history
                   WHERE idpatient = {}
                """.format(idpatient)
        return self.read_sql_query(query)


    def is_basal_level_same_next_hour(self, idpatient, timestamp):
        time_start = timestamp
        time_end = time_start + timedelta(hours=1)
        query = text("""SELECT * from basal_change_history
                        WHERE idpatient = :p1 AND
                        time_stamp > :p2 AND
                        time_stamp <= :p3
                     """)

        time_start = time_start.to_pydatetime()
        time_end = time_end.to_pydatetime()

        result = self.engine.execute(query, {'p1': idpatient,
                                             'p2': time_start,
                                             'p3': time_end})

        if (result.rowcount == 0):
            is_basal_same = True
        else:
            is_basal_same = False

        return is_basal_same
    
    def get_df_basal_change_profile(self, idpatient, start_date, end_date):
        query = text("""SELECT AVG(basal_level) as basal FROM basal_change_history
                     WHERE idpatient=:p1 AND time_stamp>:p2 
                     AND time_stamp<:p3 
                     GROUP BY hour(time_stamp)""")
        return pd.read_sql(query, self.engine, params={'p1': idpatient, 'p2': start_date, 'p3': end_date})


    def update_bolus_type(self, idbolus_history, bolus_type):
        query = text("""UPDATE bolus_history
                        SET type = :p1
                        WHERE idbolus_history = :p2
                     """)
        self.engine.execute(query, {'p1': bolus_type, 'p2': idbolus_history})


    def update_cmgm_with_opt_bolus(self, opt_bolus_type, opt_bolus_dose, opt_bolus_now, opt_bolus_later,
                   opt_bolus_duration, opt_bolus_cir, opt_number_of_meals, iob, iob_std, bolus_dose, opt_correction_dose, opt_meal_dose, misc, idcache_meal_glucose_mean):

        query = text("""UPDATE cache_meal_glucose_mean
                        SET opt_bolus_type = :p1,
                        opt_bolus_dose = :p2,
                        opt_bolus_now = :p3,
                        opt_bolus_later = :p4,
                        opt_bolus_duration = :p5,
                        opt_bolus_cir = :p6,
                        opt_number_of_meals = :p7,
                        iob = :p8,
                        iob_std = :p9,
                        bolus_dose = :p10,
                        opt_correction_dose = :p11,
                        opt_meal_dose = :p12,
                        fasting_glucose = :p13,
                        basal_at_meal = :p14,
                        basal_at_meal_2 = :p15,
                        start_time = :p16,
                        isf_multiplier = :p17
                        WHERE idcache_meal_glucose_mean = :p18
                     """)

        self.engine.execute(query, {'p1': opt_bolus_type, 
                                    'p2': self.encode_float64(opt_bolus_dose), 
                                    'p3': self.encode_int64(opt_bolus_now),
                                    'p4': self.encode_int64(opt_bolus_later),
                                    'p5': int(opt_bolus_duration), 
                                    'p6': self.encode_float64(opt_bolus_cir),
                                    'p7': int(opt_number_of_meals),
                                    'p8': self.encode_float64(iob), 
                                    'p9': self.encode_float64(iob_std),
                                    'p10': self.encode_float64(bolus_dose), 
                                    'p11': self.encode_float64(opt_correction_dose),
                                    'p12': self.encode_float64(opt_meal_dose), 
                                    'p13': self.encode_float64(misc['cgm_0']),
                                    'p14': self.encode_float64(misc['basal_at_meal']),
                                    'p15': self.encode_float64(misc['basal_at_meal_2']),
                                    'p16': misc['meal_start'],
                                    'p17': self.encode_float64(misc['p_lambda']),
                                    'p18': int(idcache_meal_glucose_mean)})


    def update_cmgm_with_correction_and_meal_dose(self, idcmgm, guide_correction_dose, guide_correction_dose_std, guide_meal_dose, guide_meal_dose_std, guide_iob, guide_iob_std):
        query = text("""UPDATE cache_meal_glucose_mean
                        SET correction_dose = :p1,
                        correction_dose_std = :p2,
                        meal_dose = :p3,
                        meal_dose_std = :p4,
                        iob = :p5,
                        iob_std = :p6
                        WHERE idcache_meal_glucose_mean = :p7""")
        self.engine.execute(query, {'p1': self.encode_float64(guide_correction_dose),
                                    'p2': self.encode_float64(guide_correction_dose_std),
                                    'p3': self.encode_float64(guide_meal_dose),
                                    'p4': self.encode_float64(guide_meal_dose_std),
                                    'p5': self.encode_float64(guide_iob),
                                    'p6': self.encode_float64(guide_iob_std),
                                    'p7': int(idcmgm)})


    def update_meal_bolus_optimization(self, idcache_meal_glucose_mean, therapy_settings_type, 
                                        opt_bolus_type, opt_meal_dose,
                                        opt_correction_dose, opt_bolus_dose, opt_bolus_now,
                                        opt_bolus_later, opt_bolus_duration):


        query = text("""insert into meal_bolus_optimization
                        (idcache_meal_glucose_mean, therapy_settings_type,
                        bolus_type, meal_dose, correction_dose, bolus_dose,
                        bolus_now, bolus_later, bolus_duration)
                        values (:p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9)
                        on duplicate key update
                        bolus_type=:p3, meal_dose=:p4, correction_dose=:p5,
                        bolus_dose=:p6, bolus_now=:p7, bolus_later=:p8, bolus_duration=:p9
                    """)
        result = self.engine.execute(query, {'p1': int(idcache_meal_glucose_mean),
                                             'p2': therapy_settings_type,
                                             'p3': opt_bolus_type,
                                             'p4': self.encode_float64(opt_meal_dose),
                                             'p5': self.encode_float64(opt_correction_dose),
                                             'p6': self.encode_float64(opt_bolus_dose),
                                             'p7': int(opt_bolus_now),
                                             'p8': int(opt_bolus_later),
                                             'p9': int(opt_bolus_duration)})
    
    def update_meal_bolus_optimization_to_unapproved(self, idpatient):
        query = text("""UPDATE meal_bolus_optimization SET approved=0 
                        WHERE idcache_meal_glucose_mean IN 
                        (SELECT idcache_meal_glucose_mean FROM cache_meal_glucose_mean WHERE idpatient=:p1)""")
        result = self.engine.execute(query, {'p1': idpatient})

    def insert_cache_bolus_analysis(self, idmeal_history, opt_correction_dose,
                                    opt_meal_dose, opt_bolus_dose,
                                    opt_bolus_now, opt_bolus_later,
                                    opt_bolus_duration, opt_bolus_type):
        query = text(""" SELECT idcache_bolus_analysis FROM cache_bolus_analysis
                      WHERE idmeal_history=:p1""")
        result = self.engine.execute(query, {'p1': int(idmeal_history)})

        if len(result.fetchall()) == 0:
            query = text("""INSERT into cache_bolus_analysis (idmeal_history,
                                                              opt_correction_dose,
                                                              opt_meal_dose,
                                                              opt_bolus_dose,
                                                              opt_bolus_now,
                                                              opt_bolus_later,
                                                              opt_bolus_duration,
                                                              opt_bolus_type)
                                values (:p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8)
                            """)
        else:
            query = text("""UPDATE cache_bolus_analysis
                             SET opt_correction_dose =:p2,
                             opt_meal_dose =:p3,
                             opt_bolus_dose =:p4,
                             opt_bolus_now =:p5,
                             opt_bolus_later =:p6,
                             opt_bolus_duration =:p7,
                             opt_bolus_type =:p8
                             WHERE idmeal_history = :p1
                        """)

        result = self.engine.execute(query, {'p1': int(idmeal_history),
                                             'p2': self.encode_float64(opt_correction_dose),
                                             'p3': self.encode_float64(opt_meal_dose),
                                             'p4': self.encode_float64(opt_bolus_dose),
                                             'p5': int(opt_bolus_now),
                                             'p6': int(opt_bolus_later),
                                             'p7': int(opt_bolus_duration),
                                             'p8': opt_bolus_type})
    def get_cache_bolus_analysis(self, idmeal_history):

        query = """SELECT *
                   FROM cache_bolus_analysis
                   WHERE idmeal_history = %s
                """ % idmeal_history
        df = self.read_sql_query(query)
        return df

    def insert_bolus_ia(self, df_ia_pot, idestimated_insulin_action):
        """value_now, value_120, number_minutes"""

        query = text("""DELETE FROM bolus_insulin_action
                        WHERE idestimated_insulin_action = :p1
                     """)

        self.engine.execute(query, {'p1': self.encode_int64(idestimated_insulin_action)})

        df_ia_pot['number_minutes'] = df_ia_pot.index.seconds // 60
        df_ia_pot['idestimated_insulin_action'] = idestimated_insulin_action
        self.to_sql(df_ia_pot, 'bolus_insulin_action', self.engine)


    def update_bolus_simulation_data(self, value_no_bolus, value_no_bolus_2, number_minutes,
                                     idcache_meal_glucose_mean):


        query = text("""SELECT max(number_minutes) FROM cache_delta_glucose 
                    WHERE idcache_meal_glucose_mean = :p1
                     """)
        result = self.engine.execute(query, {'p1': self.encode_int64(idcache_meal_glucose_mean)})

        if result.rowcount > 0:
            last_row = result.fetchone()[0]

        query = text("""UPDATE cache_delta_glucose
                        SET value_no_bolus = :p1,
                        value_no_bolus_2 = :p2
                        WHERE idcache_meal_glucose_mean = :p3
                        AND number_minutes = :p4
                     """)

        query_insert = text("""INSERT INTO cache_delta_glucose
                        (value_no_bolus, value_no_bolus_2, idcache_meal_glucose_mean, number_minutes) 
                        VALUES (:p1, :p2, :p3, :p4)
                     """)             

        for value, value_2, minute in zip(value_no_bolus, value_no_bolus_2, number_minutes):
            if minute > last_row:
                query = query_insert
            self.engine.execute(query, {'p1': self.encode_float64(value),
                                        'p2': self.encode_float64(value_2),
                                        'p3': self.encode_int64(idcache_meal_glucose_mean),
                                        'p4': self.encode_int64(minute)})


    def get_cache_delta_glucose(self, idpatient, meal_type, recipe_name = ''):
        if recipe_name:
            query = text("""SELECT number_minutes, value from cache_meal_glucose_mean cmgm 
                        JOIN cache_delta_glucose cdg ON cmgm.idcache_meal_glucose_mean=cdg.idcache_meal_glucose_mean 
                        WHERE idpatient= :p1 AND meal_type= :p2 AND recipe_name= :p3
                        """)
        else:
            query = text("""SELECT number_minutes, value from cache_meal_glucose_mean cmgm 
                    JOIN cache_delta_glucose cdg ON cmgm.idcache_meal_glucose_mean=cdg.idcache_meal_glucose_mean 
                    WHERE idpatient= :p1 AND meal_type= :p2 AND recipe_name= :p3
                     """)
        result = self.engine.execute(query, {'p1': idpatient,
                                    'p2': meal_type,
                                    'p3': recipe_name})
        df = pd.DataFrame()
        if result.rowcount > 0:
            result = result.fetchall()
            labels = ['number_minutes', 'value']
            df = pd.DataFrame.from_records(result, columns=labels)

        return df 

    def get_df_all_cache_meal_glucose_mean(self, idpatient):
        query = """SELECT idcache_meal_glucose_mean, idpatient, recipe_name, meal_type
                   FROM cache_meal_glucose_mean
                   WHERE idpatient = %s
                """ % idpatient
        df = self.read_sql_query(query)
        return df

    def get_df_full_cache_meal_glucose_mean(self, idpatient):
        query = """SELECT *
                   FROM cache_meal_glucose_mean
                   WHERE idpatient = %s
                """ % idpatient
        df = self.read_sql_query(query)
        return df

    def get_patient(self, idpatient):
        query = "SELECT * FROM patient WHERE idpatient = '%s'" %(idpatient)
        df = self.read_sql_query(query)
        return df


    def update_bmi(self, idpatient):
        # Updates bmi from the current weight and length data
        df = self.get_patient(idpatient)
        if df.loc[0]['length']>0:
            bmi = self.encode_float64(df['weight'][0]/np.square(df['length'][0]))
        else:
            bmi = None
        query = text("""UPDATE patient
                        SET bmi = :p1
                        WHERE idpatient = :p2
                     """)
        self.engine.execute(query, {'p1': bmi, 'p2': idpatient})


    def delete_all_risk_variable_values(self, idpatient):
        query = text("DELETE from variable_values where idpatient=:p1 and idpatient_variables in (select idpatient_variables from patient_variables where variable_type = 'Statistic')")
        self.engine.execute(query, {'p1': idpatient})
        log.info('DELETE from variable_values idpatient=%s where idpatient_variables in (select idpatient_variables from patient_variables where variable_type=\'Statistic\')' % idpatient)


    def delete_risk_variable_values_using_list(self, idpatient, list_patient_variables):
        patient_variables_cgm = "','".join(list_patient_variables)
        patient_variables_cgm = "'" + patient_variables_cgm + "'"

        sub_query = "select idpatient_variables from patient_variables where acronyme in (%s)" % patient_variables_cgm
        query = "delete from variable_values where idpatient=%s and idpatient_variables in (%s)" % (idpatient, sub_query)

        self.engine.execute(query)
        log.info("SQL DELETE from variable_values, idpatient=%s" % idpatient)


    def get_clinic_id_using_name(self, name):
        id = None
        query = text("Select idclinic from clinic where clinic_name = :p1")
        result = self.engine.execute(query, {'p1': name})
        if result.rowcount > 0:
            id = result.fetchone()[0]
        return id

    def get_clinic_listing(self, idpatient):
        idclinic = None
        query = text("Select idclinic from clinic_listing where idpatient = :p1")
        result = self.engine.execute(query, {'p1': idpatient})
        if result.rowcount > 0:
            idclinic = result.fetchone()[0]
        return idclinic

    def get_sensor_serial_and_model(self, idpatient, sensor_serial, sensor_model):
        query = text("select idsensor from sensor where idpatient=:p1 and serial_nr=:p2 and model=:p3")
        result = self.engine.execute(query, {'p1': idpatient, 'p2': sensor_serial, 'p3': sensor_model})
        if result.rowcount > 0:
            return result.fetchone()[0]
        else:
            return None

    def insert_cache_patient_messages(self, df):
        self.to_sql(df, 'cache_patient_messages', self.engine)

    def delete_from_cache_patient_messages(self, idpatient):
        query = text("DELETE FROM cache_patient_messages WHERE idpatient=:p1")
        self.engine.execute(query, {'p1': idpatient})

    def insert_cache_meal_glucose_mean(self, df):
        self.to_sql_insert_ignore(df, 'cache_meal_glucose_mean')


    def insert_cache_delta_glucose(self, df):
        self.to_sql(df, 'cache_delta_glucose', self.engine)


    def insert_cache_stats_day_value(self, df):
        self.to_sql(df, 'cache_stats_day_value', self.engine)


    def insert_identified_meal_into_meal_history(self, df):
        """Insert identified unannounced meals into meal_history"""

        def insert_null_recipe_into_recipe():
            """Insert null recipe and ingredient if it does not already exist in the recipe table.
            The recipe and ingredient will act as a null-object in the meals. Identified meals will
            use the null recipe."""

            id_recipe = None
            name_null_recipe = 'Unannounced Meal'
            query = "SELECT idrecipe from recipe where name='%s'" % name_null_recipe
            result = self.engine.execute(query)

            if result.rowcount > 0:
                id_recipe = result.fetchone()[0]
            else:
                result = self.insert_ingredient('null_ingredient', 'g')
                id_ingredient = result.lastrowid

                result = self.insert_recipe(name_null_recipe)
                id_recipe = result.lastrowid

                query = """INSERT into ingredients_list (idrecipe, idingredient, amount)
                           values (%s, %s, 0)""" % (id_recipe, id_ingredient)
                result = self.engine.execute(query)

            return id_recipe

        idrecipe = insert_null_recipe_into_recipe()
        df['idrecipe'] = idrecipe
        df['identified'] = 1
        df['calories'] = 0
        df['carbohydrate'] = np.nan
        df['protein'] = 0
        df['fat'] = 0
        df['alcohol'] = 0
        df['portion_size'] = 0
        self.to_sql_insert_ignore(df, 'meal_history')


    def get_all_patients_fulfilling_variable_conditions(self, id_category):
        """Go through the condition rules and see if the LATEST value that a patient has matches with
        the condition and rules that are bound to the category.

        Return None, if RuleBasedCategory does not have any variable_condition rules
        Return {} if category found, but no patient fulfills the condition.
        Return {1,2,3} if patients 1,2,3 fulfills all the conditions in that category
        """

        def build_recursive_query(list_string):
            """If we have 2 conditions that a patient has to match, we can use this query.
            We only want to select patients that fulfills both condition A and B.
            """
            if len(list_string) > 0:
                row = list_string.pop()
                if len(list_string) > 0:
                    return row + ' and t.idpatient in (' + build_recursive_query(
                        list_string) + ')'
                else:
                    return row
            else:
                return ''

        query = """select * from variable_conditions where idconditions_in_category=%s""" % id_category
        df = self.read_sql_query(query)

        set_patients = None
        if len(df) > 0:
            list_queries = []
            for i, row in df.iterrows():
                variable = row['idpatient_variables']
                operator = row['variable_relation']
                threshold = row['threshold']
                if threshold is None or np.isnan(threshold):
                    threshold = 'null'
                    mapping_new_operator = {'~': 'is not', '=': 'is'}
                    operator = mapping_new_operator[operator]

                list_queries.append("""select t.idpatient
                                       from variable_values t
                                       inner join (
                                           select idpatient, max(time_stamp) as MaxDate, idpatient_variables
                                           from variable_values
                                           where idpatient_variables=%s
                                           group by idpatient
                                       ) tm on t.idpatient = tm.idpatient and t.time_stamp = tm.MaxDate and t.idpatient_variables = tm.idpatient_variables
                                       where value %s %s
                                    """ % (variable, operator, threshold))

            query = build_recursive_query(list_queries)
            if query != '':
                result = self.engine.execute(query)
                if result.rowcount > 0:
                    patients_in_category = result.fetchall()
                    set_patients = {str(i)[1:-2] for i in patients_in_category}
                    set_patients = set(map(int, set_patients))
                else:
                    set_patients = set()

        return set_patients


    def get_all_patients_fulfilling_other_conditions(self, id_category):
        """Return None, if RuleBasedCategory does not have any variable_condition rules
           Return {} if category found, but no patient fulfills the condition.
           Return {1,2,3} if patients 1,2,3 fulfills all the conditions in that category
        """

        def convert_age_to_birth_of_date(age):
            today = date.today()
            return today - timedelta(days=(365 * age))

        def get_patients_from_resultproxy(result):
            temp_set_patients = set()
            if result.rowcount > 0:
                patients = result.fetchall()
                temp_set_patients = {str(i)[1:-2] for i in patients}
                temp_set_patients = set(map(int, temp_set_patients))

            return temp_set_patients

        list_patients = []

        query = """SELECT ov.name, oc.* from other_conditions oc
                   JOIN other_variables ov on ov.idother_variables = oc.idother_variables
                   WHERE idconditions_in_category=%s""" % id_category
        # df = pd.read_sql(query, self.engine)
        df = self.read_sql_query(query)

        if len(df) > 0:
            for i, row in df.iterrows():
                filtertype = row['name']
                row_operator = row['variable_relation']
                value = row['condition_value']
                if filtertype == 'age':
                    value = convert_age_to_birth_of_date(int(value))
                    map_operators = {'<': '>', '>': '<', '<=': '>=',
                                     '>=': '<=', '=': '=', '~': '<>'}
                    operator = map_operators[row_operator]
                    query = """select distinct idpatient from patient
                               where date_of_birth %s '%s'""" % (operator, value)
                    result = self.engine.execute(query)
                    temp_patients = get_patients_from_resultproxy(result)
                    list_patients.append(temp_patients)

                elif filtertype == 'gender':
                    query = """select distinct idpatient from patient
                               where gender %s '%s'""" % (row_operator, value)
                    result = self.engine.execute(query)
                    temp_patients = get_patients_from_resultproxy(result)
                    list_patients.append(temp_patients)

                elif filtertype == 'clinic':
                    temp = "select distinct idpatient from clinic_listing where idclinic %s" % row_operator
                    query = text(temp + " :p1")
                    result = self.engine.execute(query, {'p1': value})
                    temp_patients = get_patients_from_resultproxy(result)
                    list_patients.append(temp_patients)

            intersect_in_list_patients = set.intersection(*list_patients)
        else:
            intersect_in_list_patients = None

        return intersect_in_list_patients


    def get_all_patients_fulfilling_diagnosis_conditions(self, id_category):
        query = text("""SELECT idicd_codes
                        FROM diagnosis_conditions
                        WHERE idconditions_in_category=:p1""")
        result = self.engine.execute(query, {'p1': id_category})
        set_patients = []
        if result.rowcount > 0:
            result = result.fetchall()
            list_icd_codes = [str(i)[1:-2] for i in result]

            for idicd_codes in list_icd_codes:
                query = text("""select idpatient from diagnosis where idicd_codes = :p1""")
                result = self.engine.execute(query, {'p1': idicd_codes})
                result = result.fetchall()
                set_patients_with_idicd = set([str(i)[1:-2] for i in result])
                set_patients_with_idicd = set(map(int, set_patients_with_idicd))
                set_patients.append(set_patients_with_idicd)

            set_patients = set.intersection(*set_patients)
            return set_patients
        else:
            return None


    def get_all_patients_fulfilling_relative_deviation(self, id_category):
        # find all idpatient_variables and min_limit_threshold in the category.
        # get the patients id_variables, if it is higher than the min_limit_threshold, return that
        # patient
        query = ("""SELECT idpatient_variables, threshold
                    FROM relative_deviations
                    WHERE idconditions_in_category = %s
                """ % id_category)
        df = self.read_sql_query(query)

        if len(df) > 0 :
            set_patients = []
            for indx, row in df.iterrows():
                idpatient_variable = row['idpatient_variables']
                threshold = row['threshold']

                query2 = ("""SELECT *
                            FROM variable_values
                            WHERE idpatient_variables = %s
                        """ % idpatient_variable)
                df_var_values = self.read_sql_query(query2)

                if len(df_var_values) > 0:
                    df_groupby = df_var_values.groupby('idpatient')
                    df_std = df_groupby['value'].std()
                    df_mean = df_groupby['value'].mean()
                    df_relative_std = (df_std * 100) / abs(df_mean)
                    patients = df_relative_std.loc[df_relative_std > threshold]
                    a_patient_set = set(patients.index)
                    set_patients.append(a_patient_set)

            set_patients = set.intersection(*set_patients)
            return set_patients
        else:
            return None


    def get_patients_from_rule_based_category(self, idrule_based_category_user):
        """Returns all the patients that belongs to the RuleBasedCategory. A patient belongs to the
        RuleBasedCategory if s/he fulfills all the different condition (variable, other, diagnosis,
        relative deviation)

        Return {} if category found, but no patients fulfills all the condition.
        Return {1,2,3} if patients 1,2,3 fulfills all the conditions in that category
        """

        list_patients = []
        query = "select idconditions_in_category from conditions_in_category where idrule_based_category_user = %s" % idrule_based_category_user
        result = self.engine.execute(query)
        if result.rowcount > 0:
            result = result.fetchone()[0]
            idconditions_in_category = result

            group1 = self.get_all_patients_fulfilling_variable_conditions(idconditions_in_category)
            log.info('get all patient, variable_conditions done')
            group2 = self.get_all_patients_fulfilling_other_conditions(idconditions_in_category)
            log.info('get all patient, other_conditions done')
            group3 = self.get_all_patients_fulfilling_diagnosis_conditions(idconditions_in_category)
            log.info('get all patient, diagnosis_condition done')
            group4 = self.get_all_patients_fulfilling_relative_deviation(idconditions_in_category)
            log.info('get all patient, relative_deviation done')
            list_patients.append(group1)
            list_patients.append(group2)
            list_patients.append(group3)
            list_patients.append(group4)
            list_patients = [x for x in list_patients if x is not None]
        if len (list_patients) > 0:
            intersection_list_patients = set.intersection(*list_patients)
        else:
            intersection_list_patients = None
        return intersection_list_patients


    def get_all_variable_values_using_patients(self, patient_list):
        df = None
        if patient_list is not None and len(patient_list) > 0:
            patient_list_formatted = ",".join([str(x) for x in patient_list])
            patient_list_formatted = patient_list_formatted
            query = """SELECT time_stamp, value, idpatient_variables, idpatient
                       FROM variable_values
                       WHERE idpatient in (%s)
                    """ % patient_list_formatted
            result = self.engine.execute(query)


            if result.rowcount > 0:
                result = result.fetchall()
                labels = ['time_stamp', 'value', 'idpatient_variables', 'idpatient']
                df = pd.DataFrame.from_records(result, columns=labels)

        return df


    def insert_cache_category_rule(self, df):
        self.to_sql(df, 'cache_category_rule', self.engine)


    def insert_cache_category_user(self, df):
        self.to_sql(df, 'cache_category_user', self.engine)


    def delete_from_cache_category_rule(self, idcat):
        query = text("DELETE FROM cache_category_rule WHERE idrule_based_category_user=:p1")
        self.engine.execute(query, {'p1': idcat})


    def delete_from_cache_category_user(self, idcat):
        query = text("DELETE FROM cache_category_user WHERE iduser_categories=:p1")
        self.engine.execute(query, {'p1': idcat})


    def get_pats_in_user_cat(self, cat_id):
        query = text("SELECT idpatient FROM user_categories_patient WHERE iduser_categories=:p1")
        result = self.engine.execute(query, {'p1': cat_id})
        if result.rowcount > 0:
            result = [item[0] for item in result.fetchall()]
            return result
        else:
            return []


    def insert_cluster_pattern_basal(self, df):
        self.to_sql(df, 'cluster_pattern_basal', self.engine)


    def delete_cluster_pattern_basal(self, idpatient):
        query = text("DELETE FROM cluster_pattern_basal WHERE idpatient=:p1")
        self.engine.execute(query, {'p1': idpatient})
        log.info("delete from cluster_pattern_basal idpatient=%s" % idpatient)

    def insert_bolus_carb_into_meal_history(self, df_bolus_carb, idpatient, source):
        log.info('Inside insert to meal history')
        if len(df_bolus_carb) > 0:
            df_bolus_carb['idpatient'] = idpatient
            df_bolus_carb['source'] = source
            self.to_sql_insert_ignore(df_bolus_carb, 'meal_history')
            log.info('SQL INSERT IGNORE meal_history using bolus_carb, idpatient=%s, source=%s' % (idpatient, source))
        else:
            log.info('- SKIPPED SQL INSERT IGNORE meal_history using bolus_carb, idpatient=%s, source=%s' % (idpatient, source))

    def update_meal_history_with_bolus_guide_meal_type(self, idpatient, df):
        idrecipe = self.insert_bolus_guide_recipe_into_recipe()
        idmeal_history = df.idmeal_history.tolist()
        meal_type = df.meal_type.tolist()
        if len(idmeal_history) > 0:
            log.info('SQL UPDATE meal_history with meal_type and recipe for bolus guide meal, idpatient=%s' % (idpatient))
            for index in range(0, len(idmeal_history)):
                query = text("""UPDATE meal_history
                                SET idrecipe=:p1, meal_type=:p2
                                WHERE idmeal_history=:p3
                             """)
                result = self.engine.execute(query, {'p1': idrecipe, 'p2': meal_type[index], 'p3': idmeal_history[index]})
        return idrecipe

    def insert_bolus_guide_recipe_into_recipe(self):
        """Insert bolus guide recipe and ingredient if it does not already
        exist in the recipe table. Meals originating from bolus guide will
        use this recipe."""

        id_recipe = None
        name_bolus_recipe = 'Bolus guide Meal'
        query = "SELECT idrecipe from recipe where name='%s'" % name_bolus_recipe
        result = self.engine.execute(query)

        if result.rowcount > 0:
            id_recipe = result.fetchone()[0]
        else:
            result = self.insert_ingredient('null_ingredient', 'g')
            id_ingredient = result.lastrowid

            result = self.insert_recipe(name_bolus_recipe)
            id_recipe = result.lastrowid

            query = """INSERT into ingredients_list (idrecipe, idingredient, amount)
                       values (%s, %s, 0)""" % (id_recipe, id_ingredient)
            result = self.engine.execute(query)

        return id_recipe

    def insert_preprocessed_variable_values(self, df_preprocessed_values, idpatient, acronyme):
        query = text("SELECT idpatient_variables FROM patient_variables WHERE acronyme=:p1")
        patvar = self.engine.execute(query, {'p1': acronyme}).fetchone()

        df = pd.DataFrame(columns=['idpatient_variables', 'idpatient', 'value', 'time_stamp'])
        df['value'] = df_preprocessed_values.values
        df['time_stamp'] = df_preprocessed_values.index
        df['idpatient_variables'] = patvar[0]
        df['idpatient'] = idpatient

        self.to_sql(df, 'preprocessed_variable_values', self.engine)

    def get_preprocessed_variable_values(self, idpatient, idpatient_variables):
        query = """SELECT time_stamp, value
                   FROM preprocessed_variable_values
                   WHERE idpatient=%s and idpatient_variables=%s
                """ % (idpatient, idpatient_variables)
        df = self.read_sql_query(query)
        return df

    def delete_preprocessed_variable_values(self, idpatient, idpatient_variables):
        query = text("DELETE FROM preprocessed_variable_values WHERE idpatient=:p1 and idpatient_variables=:p2")
        self.engine.execute(query, {'p1': self.encode_int64(idpatient), 'p2': self.encode_int64(idpatient_variables)})
        log.info("DELETE from preprocessed_variable_values idpatient=%s" % idpatient)

    def get_status_existing_data(self, idpatient, end_date='2000-01-01'):

        def get_status_db(query):
            result = self.engine.execute(query).fetchone()[0]
            status = 0
            if result > 0:
                status = 1
            return status
        
        if end_date=='2000-01-01':
            # get latest date
            end_date = self.get_timestamp_last_insert_patient_variables(idpatient, 2) 
            if end_date:
                end_date+= timedelta(days=-90)


        status = {'cgm': get_status_db("""select count(*) from variable_values 
                                       where idpatient=%s and acronyme="CGM" 
                                       AND time_stamp> '%s'""" % (idpatient, end_date)),
                  'bg': get_status_db("""select count(*) from variable_values 
                                       where idpatient=%s and acronyme="BG" 
                                       AND time_stamp> '%s'""" % (idpatient, end_date)),
                  'basal': get_status_db("""select count(*) from basal_change_history 
                                       where idpatient=%s AND time_stamp> '%s'""" % (idpatient, end_date)),
                  'bolus': get_status_db("""select count(*) from bolus_history 
                                       where idpatient=%s AND time_stamp> '%s'""" % (idpatient, end_date)),
                  'meal': get_status_db("""select count(*) from meal_history 
                                       where idpatient=%s AND time_stamp> '%s'""" % (idpatient, end_date)),
                  'device': self.get_latest_bolus_device_type(idpatient)}

        return status

    def get_meal_history_bolus_carb(self, idpatient, start_date, end_date):
        query = """SELECT time_stamp, idmeal_history
                   FROM meal_history
                   WHERE idpatient=%s and time_stamp between '%s' and '%s'
                """ % (idpatient, start_date, end_date)
        df_mh_bolus_card = self.read_sql_query(query)
        return df_mh_bolus_card
    
    def get_meal_history_between_dates(self, idpatient, start_date, end_date):
        query = """SELECT *
                   FROM meal_history
                   WHERE idpatient=%s and time_stamp between '%s' and '%s'
                """ % (idpatient, start_date, end_date)
        df_mh_bolus_card = self.read_sql_query(query)
        return df_mh_bolus_card

    def insert_therapy_analysis(self, idpatient, isf, df_cir,
                                                 iob_duration, glucose_target,
                                                 start_date_entry,
                                                 end_date_entry):

        # basal analysis has already created a new ts
        query = text("""SELECT idtherapy_settings from therapy_settings 
                        WHERE idpatient = :p1 AND 
                        therapy_settings_type =\'analysis\'
                        ORDER BY updated_at DESC
                     """)
        result = self.engine.execute(query, {'p1': str(idpatient)})

        if result.rowcount > 0:
            idtherapy_settings = result.fetchone()[0]
        
        if iob_duration != 0:
            self.update_iob_duration(idpatient, iob_duration, idtherapy_settings)
        
        # Check if ts values are already written
        query = text("""SELECT count(*) from therapy_settings_value 
                WHERE idtherapy_settings=:p1 AND value_type=:p2
                """)
        result = self.engine.execute(query, {'p1': idtherapy_settings, 'p2': 'isf'})
        is_new_value = result.fetchall()[0][0]==0

        # Add isf
        if is_new_value and (isf != 0):
            start_time_isf = '00:00:00'
            self.insert_analysis_therapy_settings_values(idpatient,
                                                         idtherapy_settings,
                                                         'isf',
                                                         isf,
                                                         start_time_isf)
        # Add cir
        if is_new_value and not df_cir.empty:
            for cir_value, start_value in zip(df_cir.cir.values, df_cir.start_value.values):
                if cir_value != 0:
                    self.insert_analysis_therapy_settings_values(idpatient,
                                                                 idtherapy_settings,
                                                                 'cir',
                                                                 cir_value,
                                                                 start_value)
        # Add glucose target
        if is_new_value and (glucose_target != 0):
            start_value = '00:00:00'
            self.insert_analysis_therapy_settings_values(idpatient,
                                                         idtherapy_settings,
                                                         'glucose_target',
                                                         glucose_target,
                                                         start_value)
        if not is_new_value:  
            log.info("SKIPPED SQL INSERT of cir, iob, isf, glucose target into analysis therapy_settings, idpatient=%s" % idpatient)

            
        return idtherapy_settings

    def copy_analysis_value(self, idtherapy_settings, value_type, idpatient):
        query = text("""INSERT INTO therapy_settings_value 
                (idtherapy_settings, idpatient, profile, value_type, value, start_time, idinsulin)
                SELECT :p1, idpatient, profile, value_type, value, start_time, idinsulin 
                FROM therapy_settings_value WHERE value_type = :p2 AND idtherapy_settings =
                (SELECT idtherapy_settings FROM (
                SELECT * from therapy_settings WHERE idpatient=:p3 and therapy_settings_type='analysis' 
                ORDER BY updated_at ASC LIMIT 2) a LIMIT 1)""")
                
        result = self.engine.execute(query, {'p1': idtherapy_settings, 'p2': value_type ,'p3': idpatient})

    def get_iob_profile_data(self, brand, model):

        query = """SELECT *
                   FROM iob_profile
                   WHERE brand='%s' and model='%s'
                """ % (brand, model)
        result = self.engine.execute(query)
        res_vec = result.fetchall()
        df = pd.DataFrame()
        if len(res_vec) > 0:
            df = pd.DataFrame(res_vec)
            df.columns = result.keys()

        return df

    def get_pump_model_and_brand_for_patient(self, idpatient):

        query = """SELECT model, brand
                   FROM pump
                   WHERE idpatient=%s
                """ % (idpatient)
        result = self.engine.execute(query)

        return result.fetchall()
    
    def get_devices(self, id_devices):
        if np.ndim(id_devices) == 0:
            query = """SELECT *
                    FROM device
                    WHERE id_device = (%s)
                    """ % (id_devices)
        else:
            devices = "','".join([str(x) for x in id_devices])
            devices = "'" + devices + "'"
            query = """SELECT *
                    FROM device
                    WHERE id_device in (%s)
                    """ % (devices)
                    
        return self.read_sql_query(query)
    
    def get_latest_bolus_device_type(self, idpatient):
        query = """SELECT *
                   FROM device
                   WHERE idpatient in (%s)
                """ % (idpatient)
        df_devices = self.read_sql_query(query)
        if df_devices.empty:
            return 'pump'
        query = """SELECT id_device
                   FROM bolus_history
                   WHERE idpatient in (%s) ORDER BY time_stamp DESC LIMIT 1
                """ % (idpatient)        
        result = self.engine.execute(query)
        last_device = result.fetchone()[0]
        if last_device==None:
            return 'pump'
        return df_devices[df_devices['id_device']==last_device].type[0]
       
    def get_idinsulins_of_type(self, insulin_types):
        insulins = "','".join([str(x) for x in insulin_types])
        insulins = "'" + insulins + "'"
        query = "SELECT idinsulin FROM insulin WHERE type in (%s)" % (insulins)
        result = self.engine.execute(query)

        return [x[0] for x in result.fetchall()]
    
    def update_iob_duration(self, idpatient, iob_duration, idtherapy_settings):

        query = text("""UPDATE therapy_settings
                         SET iob_duration=:p1
                         WHERE idtherapy_settings=:p2
                     """)
        result = self.engine.execute(query, {'p1': float(iob_duration),
                                             'p2': idtherapy_settings})

    def insert_analysis_therapy_settings_values(self, idpatient,
                                                idtherapy_settings, value_type,
                                                value, start_time):
        query = text("""INSERT into therapy_settings_value (idtherapy_settings,
                                                            idpatient,
                                                            profile,
                                                            value_type,
                                                            value,
                                                            start_time)
                        VALUES (:p1, :p2, :p3, :p4, :p5, :p6)""")
        result = self.engine.execute(query, {'p1': idtherapy_settings,
                                             'p2': idpatient,
                                             'p3': 1,
                                             'p4': value_type,
                                             'p5': float(value),
                                             'p6': start_time})

    def insert_analysis_in_therapy_settings(self, idpatient, iob_duration,
                                            start_date_entry, end_date_entry):
        query = text("""INSERT into therapy_settings (idpatient,
                                                      therapy_settings_type,
                                                      treatment_type,
                                                      iob_duration,
                                                      description,
                                                      start_date_entry,
                                                      end_date_entry,
                                                      active)
                        VALUES (:p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8)""")
        result = self.engine.execute(query, {'p1': idpatient,
                                             'p2': 'analysis',
                                             'p3': 'pump',
                                             'p4': float(iob_duration),
                                             'p5': 'Default',
                                             'p6': str(start_date_entry),
                                             'p7': str(end_date_entry),
                                             'p8': 1})
        if result.rowcount > 0:
            idtherapy_settings = result.lastrowid
        else:
            idtherapy_settings = None
        return idtherapy_settings

    def update_therapy_settings_to_unapproved(self, idpatient):
        query = text("""UPDATE therapy_settings SET approved_basal_program=0,
                                approved_profile_pen_meal_bolus=0,
                                approved_profile_pen_basal=0,
                                approved_profile_isf=0,
                                approved_profile_cir=0,
                                approved_profile_glucose_target=0,
                                approved_iob_duration=0
                        WHERE idpatient=:p1""")
        result = self.engine.execute(query, {'p1': idpatient})


    def is_patient_using_bolus_guide(self, idpatient):
        query = text("""select * from bolus_guide where idpatient=:p1""")
        result = self.engine.execute(query, {'p1': self.encode_int64(idpatient)})
        return result.rowcount > 0


    def get_df_patient_variables(self):
        query = """SELECT idpatient_variables, variable_name, acronyme, units, variable_type
                   FROM patient_variables
                """
        df = self.read_sql_query(query)
        return df


    def get_df_bolus_guide(self, idpatient, end_date='2000-01-01'):
        # Protection against overflow - default to no more than 3 months
        if end_date=='2000-01-01':
            # get latest date
            end_date = self.get_latest_timestamp(idpatient, 'bolus_guide')
            if end_date:
                end_date+= timedelta(days=-90)
            
        if not isinstance(end_date,str):
            end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")
        
        query = """SELECT *
                   FROM bolus_guide
                   WHERE idpatient = %s AND time_stamp> '%s'
                """ % (idpatient, end_date)
        df = self.read_sql_query(query)
        return df
    
    def get_latest_timestamp(self,idpatient, table):
        query = """SELECT max(time_stamp)
                   FROM %s
                   WHERE idpatient = %s 
                """ % (table, idpatient)
        result = self.engine.execute(query)
        return result.fetchone()[0]
    
    def get_latest_updated_at(self,idpatient, table):
        query = """SELECT max(updated_at)
                   FROM %s
                   WHERE idpatient = %s 
                """ % (table, idpatient)
        result = self.engine.execute(query)
        return result.fetchone()[0]

### START GENERIC METHODS --------------------------------------------------------------------
    @staticmethod
    def encode_float64(val):
        #Static method to handle SQL encoding of numpy.float64
        if val is None or np.isnan(val):
            val = None
        else:
            val = ('%.15g' % val)
        return val

    @staticmethod
    def encode_int64(val):
        #Static method to handle SQL encoding of numpy.int64
        if val is None or np.isnan(val):
            val = None
        else:
            val = int(val)
        return val

    def get_count_of_table(self, table_name):
        query = 'select count(*) from %s' % table_name
        result = self.engine.execute(query)
        count = result.fetchone()[0]
        return count

    def get_first_available_id(self, table_name, col_name):
        query = "select MAX(%s) from %s" % (col_name, table_name)
        result = self.engine.execute(query)
        max = result.fetchone()[0]
        if max is None:
            return 1
        else:
            return max + 1

    def convert_sql_result_to_df(self, result):
        df = pd.DataFrame(result.fetchall())
        df.columns = result.keys()
        return df

    def to_sql(self, df, table_name, engine):
        try:
            df.to_sql(table_name, engine, if_exists='append', index=False)
        except BaseException as e:
            log.exception('insert to table ' + table_name + ' failed', exc_info=True)

    def query_database(self, query, retries=0):
        while True:
            try:
                self.engine.execute(query)
                break
            except sqlal.exc.InternalError as e:               
                if retries > 0:
                    time.sleep(2)
                    retries = retries - 1
                    self.engine.connect()
                else:
                    raise e
   
    def to_sql_insert_ignore(self, df, table):
        """
        Applies the SQL insert into 'table' on duplicate key update __ on a Dataframe.
        Does not generate any warning or errors if duplicated row is found. Instead it will just
        apply a update that does nothing.
        - This method will prevent duplicated records ONLY if a normal sql insert gives ERROR. One
        way to make sure that insert gives error is to add a composite key to the db.table, so that
        we cannot insert duplicated records.
        """
        try:
            joined_columns = ', '.join(df.columns)
            MAX_BULK_ROWS = 1000  # Value is limit by MAX_ALLOWED_PACKET in the mysql server
            counter = 0
            temp_value = ''

            for indx, row in df.iterrows():
                counter += 1
                values = row.values
                astring = []
                for val in values:
                    if isinstance(val, str):
                        astring.append('\'%s\'' % val)
                    elif isinstance(val, pd.datetime):
                        astring.append('\'%s\'' % val)
                    elif (val is None) or np.isnan(val):
                        astring.append('null')
                    else:
                        astring.append(str(val))

                df_values = ','.join(astring)
                temp_value += '(' + df_values + '),'
                if (counter % MAX_BULK_ROWS == 0 or counter == len(df)):
                    query = 'insert into %s (%s) values %s ON DUPLICATE KEY UPDATE %s=%s' % (
                    table, joined_columns, temp_value[:-1], df.columns[0], df.columns[0])
                    self.query_database(query, retries=5)
                    temp_value = ''
        except BaseException as e:
            log.exception('insert to table ' + table + ' failed', exc_info=True)

    def to_sql_with_cond(self, df, table, cond=''):

        joined_columns = ', '.join(df.columns)
        MAX_BULK_ROWS = 5000  # Value is limit by MAX_ALLOWED_PACKET in the mysql server
        counter = 0
        temp_value = ''

        for indx, row in df.iterrows():
            counter += 1
            astring = [str(v) for v in row.values]
            df_values = ','.join(astring)
            temp_value += '(' + df_values + '),'
            if (counter % MAX_BULK_ROWS == 0 or counter == len(df)):
                query = 'insert into %s (%s) values %s %s ' % (
                table, joined_columns, temp_value[:-1], cond)
                self.query_database(query, retries=2)
                temp_value = ''


    def read_sql_query(self, query):
        """Read SQL query into a DataFrame. This function is used instead of dataframe.read_sql()
        since this will make testing easier."""

        result = self.engine.execute(query)
        if query.split(' ')[0].upper() not in ['DELETE']:
            labels = result.keys()
            row = result.fetchall()
            dataframe = pd.DataFrame.from_records(row, columns=labels)
            none_cols = dataframe.isna().all()
            dataframe.loc[:, none_cols] = dataframe.loc[:, none_cols].fillna(np.nan)
            return dataframe
    
    def resample_cgm(self,df_cgm):
        # drop duplicate timestamps
        df_cgm = df_cgm.drop_duplicates(subset='time_stamp', keep='last')
        if len(df_cgm)>1:
            # Check sampling rate to determine if resampling to 5 min is needed (Freestyle Libre)
            log.debug('checking sampling rate')
            sample_rate = df_cgm.time_stamp.diff().mode()[0].total_seconds()
            log.debug('sample rate: {}'.format(sample_rate))

            if (sample_rate>400): # Well above 300 (5 min)
                log.info('Resampling CGM/FGM data to 5 minutes')
                df_cgm = df_cgm.set_index(df_cgm['time_stamp'])
                new_index = pd.to_datetime(df_cgm.index)
                log.info(df_cgm.index[0])
                df_cgm = df_cgm.reindex(new_index)
                old_idx = df_cgm.index
                new_idx = pd.date_range(old_idx.min(),old_idx.max(),freq='5Min')
                df_cgm = df_cgm.reindex(new_idx.union(old_idx))
                df_cgm = df_cgm.interpolate(limit=3).reindex(new_idx)
                df_cgm = df_cgm.drop('time_stamp',axis=1)
                df_cgm.index.rename('time_stamp',inplace=True)
                df_cgm = df_cgm.dropna()
                df_cgm = df_cgm.reset_index()
        return df_cgm
### END GENERIC METHODS --------------------------------------------------------------------


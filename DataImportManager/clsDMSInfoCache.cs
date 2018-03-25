using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using PRISM;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class DMSInfoCache : clsLoggerBase
    {
        #region "Structures"
        public struct InstrumentInfoType
        {
            public string InstrumentClass;
            public string RawDataType;
            public string CaptureType;
            public string SourcePath;
        }

        public struct OperatorInfoType
        {
            public string Name;
            public string Email;
            public string Username;
            public int UserId;
        }

        #endregion

        #region "Events"

        public event clsEventNotifier.StatusEventEventHandler DatabaseErrorEvent;

        #endregion

        #region "Member Variables"

        private readonly string mConnectionString;
        private readonly bool mTraceMode;

        /// <summary>
        /// Keys in this dictionary are error messages; values are suggested solutions to fix the error
        /// </summary>
        /// <remarks></remarks>
        private readonly Dictionary<string, string> mErrorSolutions;

        /// <summary>
        /// Keys in this dictionary are instrument names; values are instrument information
        /// </summary>
        private readonly Dictionary<string, InstrumentInfoType> mInstruments;

        /// <summary>
        /// Keys in this dictionary are username; values are operator information
        /// </summary>
        private readonly Dictionary<string, OperatorInfoType> mOperators;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks></remarks>
        public DMSInfoCache(string connectionString, bool traceMode)
        {
            mConnectionString = connectionString;
            mTraceMode = traceMode;
            mErrorSolutions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            mInstruments = new Dictionary<string, InstrumentInfoType>(StringComparer.OrdinalIgnoreCase);
            mOperators = new Dictionary<string, OperatorInfoType>(StringComparer.OrdinalIgnoreCase);
        }

        public SqlConnection GetNewDbConnection()
        {
            var retryCount = 3;
            while (retryCount > 0)
            {
                try
                {
                    if (mTraceMode)
                    {
                        ShowTraceMessage("Opening database connection using " + mConnectionString);
                    }

                    var newDbConnection = new SqlConnection(mConnectionString);
                    newDbConnection.InfoMessage += OnInfoMessage;

                    newDbConnection.Open();

                    return newDbConnection;
                }
                catch (SqlException ex)
                {
                    retryCount--;

                    LogError("Connection problem: ", ex);
                    ConsoleMsgUtils.SleepSeconds(0.5);
                }

            }

            throw new Exception("Unable to connect to the database after 3 tries");
        }

        public string GetDbErrorSolution(string errorText)
        {
            if (mErrorSolutions.Count == 0)
            {
                using (var dbConnection = GetNewDbConnection())
                {
                    LoadErrorSolutionsFromDMS(dbConnection);
                }
            }


            var query = (from item in mErrorSolutions where errorText.Contains(item.Key) select item.Value).ToList();

            if (query.Count > 0)
                return query.FirstOrDefault();

            return string.Empty;
        }

        public bool GetInstrumentInfo(string instrumentName, out InstrumentInfoType udtInstrumentInfo)
        {
            if (mInstruments.Count == 0)
            {
                using (var dbConnection = GetNewDbConnection())
                {
                    LoadInstrumentsFromDMS(dbConnection);
                }
            }

            if (mInstruments.TryGetValue(instrumentName, out udtInstrumentInfo))
                return true;

            udtInstrumentInfo = new InstrumentInfoType();
            return false;
        }

        public OperatorInfoType GetOperatorName(string operatorUsername, out int userCountMatched)
        {
            if (mOperators.Count == 0)
            {
                using (var dbConnection = GetNewDbConnection())
                {
                    LoadOperatorsFromDMS(dbConnection);
                }
            }

            var blnSuccess = LookupOperatorName(operatorUsername, out var operatorInfo, out userCountMatched);

            if (blnSuccess && !string.IsNullOrEmpty(operatorInfo.Name))
            {
                // Uncomment to debug
                //if (mTraceMode && false)
                //{
                //    ShowTraceMessage("  Operator: " + operatorInfo.Name);
                //    ShowTraceMessage("  EMail: " + operatorInfo.Email);
                //    ShowTraceMessage("  Username: " + operatorInfo.Username);
                //}

                return operatorInfo;
            }

            // No match; make sure the operator info is blank
            operatorInfo = new OperatorInfoType();

            ShowTraceMessage("  Warning: operator not found: " + operatorUsername);

            return operatorInfo;

        }

        private int GetValue(IDataRecord reader, int columnIndex, int valueIfNull)
        {
            if (reader.IsDBNull(columnIndex))
            {
                return valueIfNull;
            }

            return reader.GetInt32(columnIndex);

        }

        private string GetValue(IDataRecord reader, int columnIndex, string valueIfNull)
        {
            if (reader.IsDBNull(columnIndex))
            {
                return valueIfNull;
            }

            return reader.GetString(columnIndex);

        }

        /// <summary>
        /// Reload all DMS info now
        /// </summary>
        /// <remarks></remarks>
        // ReSharper disable once InconsistentNaming
        public void LoadDMSInfo()
        {
            using (var dbConnection = GetNewDbConnection())
            {
                LoadErrorSolutionsFromDMS(dbConnection);
                LoadInstrumentsFromDMS(dbConnection);
                LoadOperatorsFromDMS(dbConnection);
            }
        }

        // ReSharper disable once InconsistentNaming
        private void LoadErrorSolutionsFromDMS(SqlConnection dbConnection)
        {
            var retryCount = 3;
            var timeoutSeconds = 5;

            // Get a list of error messages in T_DIM_Error_Solution
            var sqlQuery =
                "SELECT Error_Text, Solution " +
                "FROM T_DIM_Error_Solution " +
                "ORDER BY Error_Text";

            if (mTraceMode)
                ShowTraceMessage("Getting error messages and solutions using " + sqlQuery);

            while (retryCount > 0)
            {
                try
                {
                    mErrorSolutions.Clear();

                    using (var cmd = new SqlCommand(sqlQuery, dbConnection))
                    {
                        cmd.CommandTimeout = timeoutSeconds;

                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {

                                var errorMessage = GetValue(reader, 0, string.Empty);
                                var solutionMessage = GetValue(reader, 1, string.Empty);

                                if (!mErrorSolutions.ContainsKey(errorMessage))
                                {
                                    mErrorSolutions.Add(errorMessage, solutionMessage);
                                }

                            }
                        }
                    }
                    break;

                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    var errorMessage = "Exception querying database in LoadErrorSolutionsFromDMS: " + ex.Message;

                    LogError(errorMessage);

                    if (retryCount > 0)
                    {
                        // Delay for 5 seconds before trying again
                        ConsoleMsgUtils.SleepSeconds(5);
                    }
                }

            } // while
        }

        // ReSharper disable once InconsistentNaming
        private void LoadInstrumentsFromDMS(SqlConnection dbConnection)
        {
            var retryCount = 3;
            var timeoutSeconds = 5;

            // Get a list of instruments in V_Instrument_List_Export
            var sqlQuery =
                 "SELECT Name, Class, RawDataType, Capture, SourcePath " +
                 "FROM dbo.V_Instrument_List_Export " +
                 "ORDER BY Name";

            if (mTraceMode)
                ShowTraceMessage("Getting instruments using " + sqlQuery);

            while (retryCount > 0)
            {
                try
                {
                    mInstruments.Clear();

                    using (var cmd = new SqlCommand(sqlQuery, dbConnection))
                    {
                        cmd.CommandTimeout = timeoutSeconds;

                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var instrumentName = GetValue(reader, 0, string.Empty)
;
                                var udtInstrumentInfo = new InstrumentInfoType
                                {
                                    InstrumentClass = GetValue(reader, 1, string.Empty),
                                    RawDataType = GetValue(reader, 2, string.Empty),
                                    CaptureType = GetValue(reader, 3, string.Empty),
                                    SourcePath = GetValue(reader, 4, string.Empty)
                                };

                                if (!mInstruments.ContainsKey(instrumentName))
                                {
                                    mInstruments.Add(instrumentName, udtInstrumentInfo);
                                }

                            }
                        }
                    }
                    break;

                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    var errorMessage = "Exception querying database in LoadInstrumentsFromDMS: " + ex.Message;

                    LogError(errorMessage);

                    if (retryCount > 0)
                    {
                        // Delay for 5 seconds before trying again
                        ConsoleMsgUtils.SleepSeconds(5);
                    }
                }

            } // while

            if (mTraceMode)
                ShowTraceMessage(" ... retrieved " + mInstruments.Count + " instruments");
        }

        // ReSharper disable once InconsistentNaming
        private void LoadOperatorsFromDMS(SqlConnection dbConnection)
        {
            var retryCount = 3;
            var timeoutSeconds = 5;

            // Get a list of all users in the database
            var sqlQuery =
                "SELECT U_Name, U_email, U_PRN, ID " +
                "FROM dbo.T_Users " +
                "ORDER BY ID desc";

            if (mTraceMode)
                ShowTraceMessage("Getting DMS users using " + sqlQuery);

            while (retryCount > 0)
            {
                try
                {
                    mOperators.Clear();

                    using (var cmd = new SqlCommand(sqlQuery, dbConnection))
                    {
                        cmd.CommandTimeout = timeoutSeconds;

                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var udtOperator = new OperatorInfoType
                                {
                                    Name = GetValue(reader, 0, string.Empty),
                                    Email = GetValue(reader, 1, string.Empty),
                                    Username = GetValue(reader, 2, string.Empty),
                                    UserId = GetValue(reader, 3, 0)
                                }
;
                                if (!string.IsNullOrWhiteSpace(udtOperator.Username) && !mOperators.ContainsKey(udtOperator.Username))
                                {
                                    mOperators.Add(udtOperator.Username, udtOperator);
                                }

                            }
                        }
                    }
                    break;

                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    var errorMessage = "Exception querying database in LoadOperatorsFromDMS: " + ex.Message;

                    LogError(errorMessage);

                    if (retryCount > 0)
                    {
                        // Delay for 5 seconds before trying again
                        ConsoleMsgUtils.SleepSeconds(5);
                    }
                }

            } // while

            if (mTraceMode)
                ShowTraceMessage(" ... retrieved " + mOperators.Count + " users");
        }

        /// <summary>
        /// Lookup the operator information given operatorUsernameToFind
        /// </summary>
        /// <param name="operatorUsernameToFind">Typically username, but could be a person's real name</param>
        /// <param name="operatorInfo">Output: Matching operator info</param>
        /// <param name="userCountMatched">Output: Number of users matched by operatorUsernameToFind</param>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks></remarks>
        private bool LookupOperatorName(string operatorUsernameToFind, out OperatorInfoType operatorInfo, out int userCountMatched)
        {
            // Get a list of all operators (hopefully just one) matching the user operator username
            if (mTraceMode)
            {
                ShowTraceMessage("Looking for operator by username: " + operatorUsernameToFind);
            }

            if (mOperators.TryGetValue(operatorUsernameToFind, out operatorInfo))
            {
                // Match found
                if (mTraceMode)
                {
                    ShowTraceMessage("Matched " + operatorInfo.Name + " using TryGetValue");
                }

                userCountMatched = 1;
                return true;
            }

            var query1 = (
                from item in mOperators
                orderby item.Value.UserId descending
                where item.Value.Username.ToLower().StartsWith(operatorUsernameToFind.ToLower())
                select item.Value).ToList();

            if (query1.Count == 1)
            {
                operatorInfo = query1.FirstOrDefault();
                var logMsg = "Matched "  + operatorInfo.Username + " using LINQ (the lookup with .TryGetValue(" + operatorUsernameToFind + ") failed)";
                LogWarning(logMsg);
                if (mTraceMode)
                {
                    ShowTraceMessage(logMsg);
                }

                userCountMatched = 1;
                return true;
            }

            // No match to an operator with username operatorUsernameToFind
            // operatorUsernameToFind may contain the person's name instead of their PRN; check for this
            // In other words, operatorUsernameToFind may be "Baker, Erin M" instead of "D3P347"
            var strQueryName = string.Copy(operatorUsernameToFind);
            if (strQueryName.IndexOf('(') > 0)
            {
                // Name likely is something like: Baker, Erin M (D3P347)
                // Truncate any text after the parenthesis
                strQueryName = strQueryName.Substring(0, strQueryName.IndexOf('(')).Trim();
            }

            var query2 = (
                from item in mOperators
                orderby item.Value.UserId descending
                where item.Value.Name.ToLower().StartsWith(strQueryName.ToLower())
                select item.Value).ToList();

            userCountMatched = query2.Count;
            if (userCountMatched == 1)
            {
                // We matched a single user
                // Update the operator name, e-mail, and PRN
                operatorInfo = query2.FirstOrDefault();
                return true;
            }

            if (userCountMatched > 1)
            {
                operatorInfo = query2.FirstOrDefault();
                var logMsg = "LookupOperatorName: Ambiguous match found for '" + strQueryName + "' in T_Users; will e-mail '" + operatorInfo.Email + "'";
                LogWarning(logMsg);

                operatorInfo.Name = "Ambiguous match found for operator (" + strQueryName + "); use network login instead, e.g. D3E154";

                // Note that the notification e-mail will get sent to operatorInfo.email
                return false;
            }
            else
            {
                // No match
                var logMsg = "LookupOperatorName: Operator not found in T_Users.U_PRN: " + operatorUsernameToFind;
                LogWarning(logMsg);

                operatorInfo.Name = "Operator [" + operatorUsernameToFind + "] not found in T_Users; should be network login name (D3E154) or full name (Moore, Ronald J)";
                return false;
            }

        }

        /// <summary>
        /// Event handler for InfoMessage event
        /// Errors and warnings sent from the SQL server are caught here
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="args"></param>
        private void OnInfoMessage(object sender, SqlInfoMessageEventArgs args)
        {
            foreach (SqlError err in args.Errors)
            {
                var s = "Message: " + err.Message +
                     ", Source: " + err.Source +
                     ", Class: " + err.Class +
                     ", State: " + err.State +
                     ", Number: " + err.Number +
                     ", LineNumber: " + err.LineNumber +
                     ", Procedure:" + err.Procedure +
                     ", Server: " + err.Server;

                DatabaseErrorEvent?.Invoke(s);
            }

        }

        private void ShowTraceMessage(string message)
        {
            clsMainProcess.ShowTraceMessage(message);
        }
    }


}

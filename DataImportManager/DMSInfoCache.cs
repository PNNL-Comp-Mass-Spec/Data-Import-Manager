using System;
using System.Collections.Generic;
using System.Linq;
using PRISMDatabaseUtils;

namespace DataImportManager
{
    // ReSharper disable once InconsistentNaming
    internal class DMSInfoCache : LoggerBase
    {
        // Ignore Spelling: desc, username

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
            public bool Obsolete;
            public int UserId;
        }

        private readonly bool mTraceMode;

        /// <summary>
        /// Keys in this dictionary are error messages; values are suggested solutions to fix the error
        /// </summary>
        private readonly Dictionary<string, string> mErrorSolutions;

        /// <summary>
        /// Keys in this dictionary are instrument names; values are instrument information
        /// </summary>
        private readonly Dictionary<string, InstrumentInfoType> mInstruments;

        /// <summary>
        /// Keys in this dictionary are username; values are operator information
        /// </summary>
        private readonly Dictionary<string, OperatorInfoType> mOperators;

        /// <summary>
        /// Constructor
        /// </summary>
        public DMSInfoCache(string connectionString, bool traceMode)
        {
            DBTools = DbToolsFactory.GetDBTools(connectionString, debugMode: traceMode);
            RegisterEvents(DBTools);

            mTraceMode = traceMode;
            mErrorSolutions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            mInstruments = new Dictionary<string, InstrumentInfoType>(StringComparer.OrdinalIgnoreCase);
            mOperators = new Dictionary<string, OperatorInfoType>(StringComparer.OrdinalIgnoreCase);
        }

        public IDBTools DBTools { get; }

        public string GetDbErrorSolution(string errorText)
        {
            if (mErrorSolutions.Count == 0)
            {
                LoadErrorSolutionsFromDMS(DBTools);
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
                LoadInstrumentsFromDMS(DBTools);
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
                LoadOperatorsFromDMS(DBTools);
            }

            var success = LookupOperatorName(operatorUsername, out var operatorInfo, out userCountMatched);

            if (success && !string.IsNullOrEmpty(operatorInfo.Name))
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

        /// <summary>
        /// Reload all DMS info now
        /// </summary>
        // ReSharper disable once InconsistentNaming
        public void LoadDMSInfo()
        {
            LoadErrorSolutionsFromDMS(DBTools);
            LoadInstrumentsFromDMS(DBTools);
            LoadOperatorsFromDMS(DBTools);
        }

        // ReSharper disable once InconsistentNaming
        private void LoadErrorSolutionsFromDMS(IDBTools dbTools)
        {
            const short retryCount = 3;
            const int timeoutSeconds = 5;

            // Get a list of error messages in T_DIM_Error_Solution
            const string sqlQuery =
                "SELECT error_text, solution " +
                "FROM T_DIM_Error_Solution " +
                "ORDER BY error_text";

            if (mTraceMode)
                ShowTraceMessage("Getting error messages and solutions using " + sqlQuery);

            var success = dbTools.GetQueryResults(sqlQuery, out var results, retryCount, timeoutSeconds: timeoutSeconds);

            if (!success)
                LogWarning("GetQueryResults returned false querying T_DIM_Error_Solution");

            foreach (var row in results)
            {
                var errorMessage = row[0];
                var solutionMessage = row[1];

                if (!mErrorSolutions.ContainsKey(errorMessage))
                {
                    mErrorSolutions.Add(errorMessage, solutionMessage);
                }
            }
        }

        // ReSharper disable once InconsistentNaming
        private void LoadInstrumentsFromDMS(IDBTools dbTools)
        {
            const short retryCount = 3;
            const int timeoutSeconds = 5;

            // Get a list of instruments in V_Instrument_List_Export
            const string sqlQuery =
                "SELECT name, class, raw_data_type, capture, source_path " +
                "FROM V_Instrument_List_Export " +
                "ORDER BY name";

            if (mTraceMode)
                ShowTraceMessage("Getting instruments using " + sqlQuery);

            var success = dbTools.GetQueryResults(sqlQuery, out var results, retryCount, timeoutSeconds: timeoutSeconds);

            if (!success)
                LogWarning("GetQueryResults returned false querying V_Instrument_List_Export");

            foreach (var row in results)
            {
                var instrumentName = row[0];

                var udtInstrumentInfo = new InstrumentInfoType
                {
                    InstrumentClass = row[1],
                    RawDataType = row[2],
                    CaptureType = row[3],
                    SourcePath = row[4]
                };

                if (!mInstruments.ContainsKey(instrumentName))
                {
                    mInstruments.Add(instrumentName, udtInstrumentInfo);
                }
            }

            if (mTraceMode)
                ShowTraceMessage(" ... retrieved " + mInstruments.Count + " instruments");
        }

        // ReSharper disable once InconsistentNaming
        private void LoadOperatorsFromDMS(IDBTools dbTools)
        {
            const short retryCount = 3;
            const int timeoutSeconds = 5;

            // Get a list of all users in the database
            const string sqlQuery =
                "SELECT name, email, username, id, status " +
                "FROM V_Users_Export " +
                "ORDER BY id Desc";

            if (mTraceMode)
                ShowTraceMessage("Getting DMS users using " + sqlQuery);

            var success = dbTools.GetQueryResults(sqlQuery, out var results, retryCount, timeoutSeconds: timeoutSeconds);

            if (!success)
                LogWarning("GetQueryResults returned false querying T_Users");

            foreach (var row in results)
            {
                if (!int.TryParse(row[3], out var userId))
                {
                    userId = 0;
                }

                var udtOperator = new OperatorInfoType
                {
                    Name = row[0],
                    Email = row[1],
                    Username = row[2],
                    UserId = userId,
                    Obsolete = string.Equals(row[4], "Obsolete", StringComparison.OrdinalIgnoreCase)
                };

                if (!string.IsNullOrWhiteSpace(udtOperator.Username) && !mOperators.ContainsKey(udtOperator.Username.ToUpper()))
                {
                    mOperators.Add(udtOperator.Username.ToUpper(), udtOperator);
                }
            }

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
        private bool LookupOperatorName(string operatorUsernameToFind, out OperatorInfoType operatorInfo, out int userCountMatched)
        {
            // Get a list of all operators (hopefully just one) matching the user operator username
            if (mTraceMode)
            {
                ShowTraceMessage("Looking for operator by username: " + operatorUsernameToFind);
            }

            // operatorUsernameToFind may contain the person's name instead of their PRN; check for this
            // In other words, operatorUsernameToFind may be "Baker, Erin M" instead of "D3P347"
            var queryName = string.Copy(operatorUsernameToFind);
            var queryUsername = string.Copy(operatorUsernameToFind);

            if (queryName.IndexOf('(') > 0)
            {
                // Name likely is something like: Baker, Erin M (D3P347)
                // Truncate any text after the parenthesis for the name
                queryName = queryName.Substring(0, queryName.IndexOf('(')).Trim();

                // Only keep what's inside the parentheses for the username
                // This is not a comprehensive check; it assumes there is nothing in the string after the user's name besides the username, surrounded by parentheses and spaces
                queryUsername = queryUsername.Substring(queryName.Length).Trim(' ', '(', ')');
            }

            if (mOperators.TryGetValue(queryUsername.ToUpper(), out operatorInfo))
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
                from item in mOperators.Values
                orderby item.UserId descending
                where item.Username.StartsWith(queryUsername, StringComparison.OrdinalIgnoreCase)
                select item).ToList();

            if (query1.Count == 1)
            {
                operatorInfo = query1.FirstOrDefault();
                var logMsg = "Matched "  + operatorInfo.Username + " using LINQ (the lookup with .TryGetValue(" + queryUsername + ") failed)";
                LogWarning(logMsg);

                if (mTraceMode)
                {
                    ShowTraceMessage(logMsg);
                }

                userCountMatched = 1;
                return true;
            }

            // No match to an operator with username operatorUsernameToFind
            // See if a user's name was supplied instead

            var query2 = (
                from item in mOperators.Values
                orderby item.UserId descending
                where item.Name.StartsWith(queryName, StringComparison.OrdinalIgnoreCase)
                select item).ToList();

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
                var nonObsolete = query2.Where(item => !item.Obsolete).ToList();

                if (nonObsolete.Count == 1)
                {
                    // We only matched a single non-obsolete user
                    // Update the operator name, e-mail, and PRN
                    operatorInfo = nonObsolete.FirstOrDefault();
                    return true;
                }

                var exactMatch = nonObsolete.Where(item => item.Name.Equals(queryName, StringComparison.OrdinalIgnoreCase)).ToList();

                if (exactMatch.Count == 1)
                {
                    // We do have an exact match to a single non-obsolete user
                    // Update the operator name, e-mail, and PRN
                    operatorInfo = exactMatch.FirstOrDefault();
                    return true;
                }

                operatorInfo = query2.FirstOrDefault();
                var logMsg = "LookupOperatorName: Ambiguous match found for '" + queryName + "' in V_Users_Export; will e-mail '" + operatorInfo.Email + "'";
                LogWarning(logMsg);

                operatorInfo.Name = "Ambiguous match found for operator (" + queryName + "); use network login instead, e.g. D3E154";

                // Note that the notification e-mail will get sent to operatorInfo.email
                return false;
            }
            else
            {
                // No match
                var logMsg = "LookupOperatorName: Operator not found in V_Users_Export.username: " + operatorUsernameToFind;
                LogWarning(logMsg);

                operatorInfo.Name = "Operator [" + operatorUsernameToFind + "] not found in V_Users_Export; should be network login name (D3E154) or full name (Moore, Ronald J)";
                return false;
            }
        }

        private void ShowTraceMessage(string message)
        {
            MainProcess.ShowTraceMessage(message);
        }
    }
}

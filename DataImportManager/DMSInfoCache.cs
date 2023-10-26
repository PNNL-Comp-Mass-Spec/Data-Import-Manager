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

        /// <summary>
        /// Instrument info
        /// </summary>
        public struct InstrumentInfoType
        {
            /// <summary>
            /// Instrument class
            /// </summary>
            /// <remarks>Examples: LTQ_FT, Triple_Quad, BrukerFT_BAF</remarks>
            public string InstrumentClass;

            /// <summary>
            /// Raw data type
            /// </summary>
            /// <remarks>Examples: dot_raw_files, dot_d_folders, dot_uimf_files, </remarks>
            public string RawDataType;

            /// <summary>
            /// Capture type
            /// </summary>
            /// <remarks>secfso for an instrument on bionet; fso for an instrument on the pnl.gov domain</remarks>
            public string CaptureType;

            /// <summary>
            /// Source path
            /// </summary>
            /// <remarks>Examples: \\Lumos02.bionet\ProteomicsData\ and \\Proto-2\External_Lumos_Xfer\</remarks>
            public string SourcePath;

            /// <summary>
            /// Show the instrument class name
            /// </summary>
            /// <returns></returns>
            public override string ToString()
            {
                return string.Format("{0}", InstrumentClass ?? "Undefined instrument class name");
            }
        }

        /// <summary>
        /// DMS user info
        /// </summary>
        public struct UserInfoType
        {
            /// <summary>
            /// DMS user's given name
            /// </summary>
            public string Name;

            /// <summary>
            /// DMS user's given e-mail
            /// </summary>
            public string Email;

            /// <summary>
            /// DMS user's given username
            /// </summary>
            public string Username;

            /// <summary>
            /// This is true if the DMS user's status is "Obsolete"
            /// </summary>
            public bool Obsolete;

            /// <summary>
            /// DMS User ID
            /// </summary>
            public int UserId;

            /// <summary>
            /// Show the DMS user's name and username
            /// </summary>
            public override string ToString()
            {
                return string.Format("{0} ({1})", Name ?? "Undefined name", Username ?? "Undefined username");
            }
        }

        // ReSharper disable once InconsistentNaming

        /// <summary>
        /// This is set to true after LoadDMSInfo is called
        /// </summary>
        public bool DMSInfoLoaded { get; private set; }

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
        /// Keys in this dictionary are username; values are DMS user information
        /// </summary>
        private readonly Dictionary<string, UserInfoType> mDmsUsers;

        /// <summary>
        /// Constructor
        /// </summary>
        public DMSInfoCache(string connectionString, bool traceMode)
        {
            DBTools = DbToolsFactory.GetDBTools(connectionString, debugMode: traceMode);
            RegisterEvents(DBTools);

            DMSInfoLoaded = false;

            mTraceMode = traceMode;
            mErrorSolutions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            mInstruments = new Dictionary<string, InstrumentInfoType>(StringComparer.OrdinalIgnoreCase);
            mDmsUsers = new Dictionary<string, UserInfoType>(StringComparer.OrdinalIgnoreCase);
        }

        // ReSharper disable once InconsistentNaming

        /// <summary>
        /// Instance of DbToolsFactory for querying the database
        /// </summary>
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

        /// <summary>
        /// Determine a DMS user given a username
        /// </summary>
        /// <param name="usernameToFind">Typically username, but could be a person's real name</param>
        /// <param name="userCountMatched">Output: Number of users matched by usernameToFind</param>
        /// <returns>DMS user info</returns>
        public UserInfoType GetUserInfo(string usernameToFind, out int userCountMatched)
        {
            if (mDmsUsers.Count == 0)
            {
                LoadUsersFromDMS(DBTools);
            }

            var success = LookupUserInfo(usernameToFind, out var userInfo, out userCountMatched);

            if (success && !string.IsNullOrEmpty(userInfo.Name))
            {
                // Uncomment to debug
                //if (mTraceMode && false)
                //{
                //    ShowTraceMessage("  User: " + userInfo.Name);
                //    ShowTraceMessage("  EMail: " + userInfo.Email);
                //    ShowTraceMessage("  Username: " + userInfo.Username);
                //}

                return userInfo;
            }

            // No match

            ShowTraceMessage("  Warning: user not found: " + userInfo);

            return new UserInfoType(); ;
        }

        /// <summary>
        /// Reload all DMS info now
        /// </summary>
        // ReSharper disable once InconsistentNaming
        public void LoadDMSInfo()
        {
            if (DMSInfoLoaded)
                return;

            LoadErrorSolutionsFromDMS(DBTools);
            LoadInstrumentsFromDMS(DBTools);
            LoadUsersFromDMS(DBTools);

            DMSInfoLoaded = true;
        }

        // ReSharper disable once InconsistentNaming
        private void LoadErrorSolutionsFromDMS(IDBTools dbTools)
        {
            const int timeoutSeconds = 5;

            // Get a list of error messages in T_DIM_Error_Solution
            const string sqlQuery =
                "SELECT error_text, solution " +
                "FROM T_DIM_Error_Solution " +
                "ORDER BY error_text";

            if (mTraceMode)
                ShowTraceMessage("Getting error messages and solutions using " + sqlQuery);

            var success = dbTools.GetQueryResults(sqlQuery, out var results, timeoutSeconds: timeoutSeconds);

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
            const int timeoutSeconds = 5;

            // Get a list of instruments in V_Instrument_List_Export
            const string sqlQuery =
                "SELECT name, class, raw_data_type, capture, source_path " +
                "FROM V_Instrument_List_Export " +
                "ORDER BY name";

            if (mTraceMode)
                ShowTraceMessage("Getting instruments using " + sqlQuery);

            var success = dbTools.GetQueryResults(sqlQuery, out var results, timeoutSeconds: timeoutSeconds);

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
        private void LoadUsersFromDMS(IDBTools dbTools)
        {
            const int timeoutSeconds = 5;

            // Get a list of all users in the database
            const string sqlQuery =
                "SELECT name, email, username, id, status " +
                "FROM V_Users_Export " +
                "ORDER BY id Desc";

            if (mTraceMode)
                ShowTraceMessage("Getting DMS users using " + sqlQuery);

            var success = dbTools.GetQueryResults(sqlQuery, out var results, timeoutSeconds: timeoutSeconds);

            if (!success)
                LogWarning("GetQueryResults returned false querying V_Users_Export");

            foreach (var row in results)
            {
                if (!int.TryParse(row[3], out var userId))
                {
                    userId = 0;
                }

                var udtUser = new UserInfoType
                {
                    Name = row[0],
                    Email = row[1],
                    Username = row[2],
                    UserId = userId,
                    Obsolete = string.Equals(row[4], "Obsolete", StringComparison.OrdinalIgnoreCase)
                };

                if (!string.IsNullOrWhiteSpace(udtUser.Username) && !mDmsUsers.ContainsKey(udtUser.Username.ToUpper()))
                {
                    mDmsUsers.Add(udtUser.Username.ToUpper(), udtUser);
                }
            }

            if (mTraceMode)
                ShowTraceMessage(" ... retrieved " + mDmsUsers.Count + " users");
        }

        /// <summary>
        /// Lookup the user information given usernameToFind
        /// </summary>
        /// <param name="usernameToFind">Typically username, but could be a person's real name</param>
        /// <param name="userInfo">Output: Matching user info</param>
        /// <param name="userCountMatched">Output: Number of users matched by usernameToFind</param>
        /// <returns>True if success, otherwise false</returns>
        private bool LookupUserInfo(string usernameToFind, out UserInfoType userInfo, out int userCountMatched)
        {
            // Get a list of all users (hopefully just one) matching the user's username
            if (mTraceMode)
            {
                ShowTraceMessage("Looking for DMS user by username: " + usernameToFind);
            }

            // usernameToFind may contain the person's name instead of their PRN; check for this
            // In other words, usernameToFind may be "Baker, Erin M" instead of "D3P347"
            var queryName = string.Copy(usernameToFind);
            var queryUsername = string.Copy(usernameToFind);

            if (queryName.IndexOf('(') > 0)
            {
                // Name likely is something like: Baker, Erin M (D3P347)
                // Truncate any text after the parenthesis for the name
                queryName = queryName.Substring(0, queryName.IndexOf('(')).Trim();

                // Only keep what's inside the parentheses for the username
                // This is not a comprehensive check; it assumes there is nothing in the string after the user's name besides the username, surrounded by parentheses and spaces
                queryUsername = queryUsername.Substring(queryName.Length).Trim(' ', '(', ')');
            }

            if (mDmsUsers.TryGetValue(queryUsername.ToUpper(), out userInfo))
            {
                // Match found
                if (mTraceMode)
                {
                    ShowTraceMessage("Matched " + userInfo.Name + " using TryGetValue");
                }

                userCountMatched = 1;
                return true;
            }

            var query1 = (
                from item in mDmsUsers.Values
                orderby item.UserId descending
                where item.Username.StartsWith(queryUsername, StringComparison.OrdinalIgnoreCase)
                select item).ToList();

            if (query1.Count == 1)
            {
                userInfo = query1.FirstOrDefault();
                var logMsg = "Matched "  + userInfo.Username + " using LINQ (the lookup with .TryGetValue(" + queryUsername + ") failed)";
                LogWarning(logMsg);

                if (mTraceMode)
                {
                    ShowTraceMessage(logMsg);
                }

                userCountMatched = 1;
                return true;
            }

            // No match to a DMS user with username usernameToFind
            // See if a user's name was supplied instead

            var query2 = (
                from item in mDmsUsers.Values
                orderby item.UserId descending
                where item.Name.StartsWith(queryName, StringComparison.OrdinalIgnoreCase)
                select item).ToList();

            userCountMatched = query2.Count;

            if (userCountMatched == 1)
            {
                // We matched a single user
                // Update the user's name, e-mail, and username
                userInfo = query2.FirstOrDefault();
                return true;
            }

            if (userCountMatched > 1)
            {
                var nonObsolete = query2.Where(item => !item.Obsolete).ToList();

                if (nonObsolete.Count == 1)
                {
                    // We only matched a single non-obsolete user
                    // Update the user's name, e-mail, and username
                    userInfo = nonObsolete.FirstOrDefault();
                    return true;
                }

                var exactMatch = nonObsolete.Where(item => item.Name.Equals(queryName, StringComparison.OrdinalIgnoreCase)).ToList();

                if (exactMatch.Count == 1)
                {
                    // We do have an exact match to a single non-obsolete user
                    // Update the user's name, e-mail, and username
                    userInfo = exactMatch.FirstOrDefault();
                    return true;
                }

                userInfo = query2.FirstOrDefault();
                var logMsg = "LookupUserInfo: Ambiguous match found for '" + queryName + "' in V_Users_Export; will e-mail '" + userInfo.Email + "'";
                LogWarning(logMsg);

                userInfo.Name = "Ambiguous match found for user (" + queryName + "); use network login instead, e.g. D3E154";

                // Note that the notification e-mail will get sent to userInfo.email
                return false;
            }
            else
            {
                // No match
                var logMsg = "LookupUserInfo: User not found in V_Users_Export.username: " + usernameToFind;
                LogWarning(logMsg);

                userInfo.Name = "User [" + usernameToFind + "] not found in V_Users_Export; should be network login name (D3E154) or full name (Moore, Ronald J)";
                return false;
            }
        }

        private void ShowTraceMessage(string message)
        {
            MainProcess.ShowTraceMessage(message);
        }
    }
}

using CsvHelper;
using LoadProfile;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Diagnostics;
using System.Globalization;

internal class Program
{
    private static void Main(string[] args)
    {
        int current = 0;
        Stopwatch stopwatch = Stopwatch.StartNew();
        var listOfMeters = _fileReader();
        DateTime today = DateTime.Today;
        DateTime start = new DateTime(today.Year, today.Month, 1).AddMonths(-1);
        DateTime end = start.AddMonths(1).AddMinutes(-1);
        var loadProfiles = new List<LoadProfileDTO>();

        //var data = _getLoadProfileData(listOfMeters, start, end);	

        using (SqlConnection connection = new SqlConnection(Constants.connectionString))
        {
            connection.Open();
            Console.WriteLine("Connection to Db Successful");
            listOfMeters.ForEach(meter =>
            {
                var meterLatestLoadProfileData = _timeMachine(meter.MeterNumber, end, connection);
                current = current + 1;
                Console.WriteLine("Row {0} Completed", current);
                if (meterLatestLoadProfileData != null)
                {
                    Console.WriteLine("Row {0} has Data", current);
                    meter.ReadingPeriod = meterLatestLoadProfileData.Period;
                }
                else Console.WriteLine("No Record was found for Meter Number {0}", meter.MeterNumber);
            });
            Console.WriteLine("Closing connection to Database.........");
        }
        _fileWriter(listOfMeters);
        stopwatch.Stop();
        Console.WriteLine("Elapsed time: {0} ms", stopwatch.ElapsedMilliseconds);
    }

    private static LoadProfileDTO _getLoadProfileData(string meterNumber, DateTime start, DateTime end, SqlConnection connection)
    {
        LoadProfileDTO response = null;
        using (SqlCommand command = new SqlCommand(Constants.storedProcedure, connection))
        {
            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.Add(new SqlParameter("@MeterNumber", SqlDbType.NVarChar) { Value = meterNumber });
            command.Parameters.Add(new SqlParameter("@StartDate", SqlDbType.DateTime) { Value = start });
            command.Parameters.Add(new SqlParameter("@EndDate", SqlDbType.DateTime) { Value = end });
            using (SqlDataAdapter adapter = new SqlDataAdapter(command))
            {
                DataTable dataTable = new DataTable();
                adapter.Fill(dataTable);
                if (dataTable.AsEnumerable().Any())
                {
                    var data = dataTable.AsEnumerable().Select(x => new LoadProfileDTO
                    {
                        MeterNumber = x[0].ToString(),
                        Description = x[1].ToString(),
                        Period = (DateTime)x[2],
                        Pnet = x[3].ToString(),
                        Snet = x[4].ToString(),
                    }).ToList();
                    var loadProfileData = data.OrderBy(x => x.Period);
                    //Pick First and Add to List on Top
                    response = loadProfileData.Last();
                }
            }
        }
        return response;
    }

    private static List<Meters> _getLoadProfileData(List<Meters> meters, DateTime start, DateTime end)
    {
        int index = 0;
        List<Meters> response = new List<Meters>();
        using (SqlConnection connection = new SqlConnection(Constants.connectionString))
        {
            connection.Open();
            using (SqlCommand command = new SqlCommand(Constants.storedProcedure, connection))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.Add(new SqlParameter("@MeterNumber", SqlDbType.NVarChar));
                command.Parameters.Add(new SqlParameter("@StartDate", SqlDbType.DateTime));
                command.Parameters.Add(new SqlParameter("@EndDate", SqlDbType.DateTime));

                foreach (var meter in meters)
                {
                    index = index + 1;
                    command.Parameters["@MeterNumber"].Value = meter.MeterNumber;
                    command.Parameters["@StartDate"].Value = start;
                    command.Parameters["@EndDate"].Value = end;
                    using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                    {
                        DataTable dataTable = new DataTable();
                        adapter.Fill(dataTable);
                        if (dataTable.AsEnumerable().Any())
                        {
                            var data = dataTable.AsEnumerable().Select(x => new LoadProfileDTO
                            {
                                Period = (DateTime)x[2],
                            }).ToList();
                            var loadProfileData = data.OrderBy(x => x.Period);
                            var latest = loadProfileData.Last();
                            meter.ReadingPeriod = latest.Period;
                        }
                    }
                    Console.WriteLine("Row {0} Completed", index);
                }
            }
        }
        return response;
    }

    private static LoadProfileDTO _timeMachine(string meterNumber, DateTime end, SqlConnection connection)
    {
        var response = new LoadProfileDTO();
        var beginningOfMonth = new DateTime(end.Year, end.Month, 1);
        var daysDifference = (end - beginningOfMonth).TotalDays;
        if (daysDifference > 3)
        {
            DateTime newStartDate = end.AddDays(-3);
            DateTime newEndDate = end;
            var loadprofile = _getLoadProfileData(meterNumber, newStartDate, newEndDate, connection);
            if (loadprofile == null) return _timeMachine(meterNumber, newStartDate, connection);
            return loadprofile;
        }
        else
        {
            DateTime start = new DateTime(end.Year, end.Month, 1);
            DateTime newEndDate = end;
            var loadprofileFinal = _getLoadProfileData(meterNumber, start, end, connection);
            return loadprofileFinal;
        }
    }

    private static void _fileWriter(List<Meters> meters)
    {
        Console.WriteLine("Trying to write records to file.......");
        using (var writer = new StreamWriter(Constants.writePath))
        using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
        {
            csv.WriteRecords(meters);
        }
    }

    private static List<Meters> _fileReader()
    {
        Console.WriteLine("Trying to read records from file.......");
        var listOfMeters = new List<Meters>();
        using (var reader = new StreamReader(Constants.readPath))
        using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
        {
            var records = csv.GetRecords<Meters>();
            listOfMeters.AddRange(records);
        }
        return listOfMeters;
    }
}
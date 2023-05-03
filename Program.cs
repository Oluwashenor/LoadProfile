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
        var listOfMeters = FileReader();
        DateTime today = DateTime.Today;
        DateTime start = new DateTime(today.Year, today.Month, 1).AddMonths(-1);
        DateTime end = start.AddMonths(1).AddMinutes(-1);
        var loadProfiles = new List<LoadProfileDTO>();

        //var data = _getLoadProfileData(listOfMeters, start, end);	

        using (SqlConnection connection = new(Constants.connectionString))
        {
            connection.Open();
            Console.WriteLine("Connection to Db Successful");
            listOfMeters.ForEach(meter =>
            {
                var meterLatestLoadProfileData = TimeMachine(meter.MeterNumber, end, connection);
                current++;
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
        FileWriter(listOfMeters);
        stopwatch.Stop();
        Console.WriteLine("Elapsed time: {0} ms", stopwatch.ElapsedMilliseconds);
    }

    private static LoadProfileDTO? GetLoadProfileData(string meterNumber, DateTime start, DateTime end, SqlConnection connection)
    {
        LoadProfileDTO? response = null;
        using (SqlCommand command = new(Constants.storedProcedure, connection))
        {
            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.Add(new SqlParameter("@MeterNumber", SqlDbType.NVarChar) { Value = meterNumber });
            command.Parameters.Add(new SqlParameter("@StartDate", SqlDbType.DateTime) { Value = start });
            command.Parameters.Add(new SqlParameter("@EndDate", SqlDbType.DateTime) { Value = end });
            using SqlDataAdapter adapter = new SqlDataAdapter(command);
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
        return response;
    }

    //Not using this, nothing wrong with it just not using it
    private static List<Meters> GetLoadProfileData(List<Meters> meters, DateTime start, DateTime end)
    {
        int index = 0;
        List<Meters> response = new();
        using (SqlConnection connection = new(Constants.connectionString))
        {
            connection.Open();
            using SqlCommand command = new(Constants.storedProcedure, connection);
            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.Add(new SqlParameter("@MeterNumber", SqlDbType.NVarChar));
            command.Parameters.Add(new SqlParameter("@StartDate", SqlDbType.DateTime));
            command.Parameters.Add(new SqlParameter("@EndDate", SqlDbType.DateTime));

            foreach (var meter in meters)
            {
                index++;
                command.Parameters["@MeterNumber"].Value = meter.MeterNumber;
                command.Parameters["@StartDate"].Value = start;
                command.Parameters["@EndDate"].Value = end;
                using (SqlDataAdapter adapter = new(command))
                {
                    DataTable dataTable = new();
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
        return response;
    }

    private static LoadProfileDTO? TimeMachine(string meterNumber, DateTime end, SqlConnection connection)
    {
        var beginningOfMonth = new DateTime(end.Year, end.Month, 1);
        var daysDifference = (end - beginningOfMonth).TotalDays;
        if (daysDifference > 3)
        {
            DateTime newStartDate = end.AddDays(-3);
            DateTime newEndDate = end;
            var loadprofile = GetLoadProfileData(meterNumber, newStartDate, newEndDate, connection);
            if (loadprofile == null) return TimeMachine(meterNumber, newStartDate, connection);
            return loadprofile;
        }
        else
        {
            DateTime start = new(end.Year, end.Month, 1);
            var loadprofileFinal = GetLoadProfileData(meterNumber, start, end, connection);
            return loadprofileFinal;
        }
    }

    //This is for writing the data to file after operation has been completed
    private static void FileWriter(List<Meters> meters)
    {
        Console.WriteLine("Trying to write records to file.......");
        using (var writer = new StreamWriter(Constants.writePath))
        using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
        {
            csv.WriteRecords(meters);
        }
    }

    // This is for reading the file and picking out the Meter Numbers
    private static List<Meters> FileReader()
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
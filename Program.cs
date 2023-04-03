using Azure;
using CsvHelper;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Formats.Asn1;
using System.Globalization;

internal class Program
{
	private static void Main(string[] args)
	{
		int current = 0;
		Stopwatch stopwatch = Stopwatch.StartNew();
		var listOfMeters = new List<Meters>();
		DateTime today = DateTime.Today;
		DateTime start = new DateTime(today.Year, today.Month, 1).AddMonths(-1);
		DateTime end = start.AddMonths(1).AddMinutes(-1);

		var loadProfiles = new List<LoadProfileDTO>();
		var readPath = @"c:\Users\Shenor\Desktop\shenor\CSV\loadProfileMeter.csv";
		using (var reader = new StreamReader(readPath))
		using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
		{
			var records = csv.GetRecords<Meters>();
			listOfMeters.AddRange(records);
		}
		listOfMeters.ForEach(meter =>
		{
			//var meterLatestLoadProfileData = _timeMachine(meter.MeterNumber, start, end); 
			var meterLatestLoadProfileData = _getLoadProfileData(meter.MeterNumber, start, end);
			current = current + 1;
			Console.WriteLine("Row {0} Completed",current);
			if (meterLatestLoadProfileData != null)
			{
				Console.WriteLine("Row {0} has Data", current);
				meter.ReadingPeriod = meterLatestLoadProfileData.Period;
			}
			else Console.WriteLine("No Record was found for Meter Number {0}", meter.MeterNumber);
		});
		var writePath = @"c:\Users\Shenor\Desktop\shenor\CSV\meterWriter.csv";
		using (var writer = new StreamWriter(writePath))
		using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
		{
			csv.WriteRecords(listOfMeters);
		}
		stopwatch.Stop();
		Console.WriteLine("Elapsed time: {0} ms", stopwatch.ElapsedMilliseconds);
	}

	private static LoadProfileDTO _getLoadProfileData(string meterNumber, DateTime start, DateTime end)
	{
		string connectionString = "Server=(localdb)\\mssqllocaldb;Database=Chandler;Trusted_Connection=True;MultipleActiveResultSets=true";
		
		LoadProfileDTO response = null;
		using (SqlConnection connection = new SqlConnection(connectionString))
		{
			connection.Open();
			using (SqlCommand command = new SqlCommand("GetLoadProfileData_with_time", connection))
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
		}
		return response;
	}


	private static LoadProfileDTO _timeMachine(string meterNumber,DateTime start, DateTime end)
	{
		var beginningOfDate = new DateTime(end.Year, end.Month, 1);
		var daysDifference = (end - beginningOfDate).TotalDays;
		if (daysDifference > 3)
		{
			DateTime newStartDate = end.AddDays(-3);
			DateTime newEndDate = end;
			var loadprofile = _getLoadProfileData(meterNumber, newStartDate, newEndDate);
			loadprofile = null;
			if(loadprofile == null)
			{
				_timeMachine(meterNumber, newStartDate, newEndDate);
			}
			return loadprofile;
		}
		else
		{
			DateTime newStartDate = new DateTime(end.Year, end.Month, 1);
			DateTime newEndDate = end;
			var loadprofile = _getLoadProfileData(meterNumber, start, end);
			return loadprofile;
		}
		
		
	}

}
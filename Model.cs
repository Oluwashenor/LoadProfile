using System;

public class Meters
{
	public string MeterNumber { get; set; }
	public DateTime? ReadingPeriod { get; set; }
}

public class LoadProfileDTO
{
    public string? MeterNumber { get; set; }
    public DateTime Period { get; set; }
    public string? Description { get; set; }
    public string? Pnet { get; set; }
    public string? Snet { get; set; }
}
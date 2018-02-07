package spark;

import java.io.Serializable;

public class CallLog implements Serializable {
	private static final long serialVersionUID = 2597105495282743206L;
	public String callsign;
	public Double contactlat;
	public Double contactlong;
	public Double mylat;
	public Double mylong;
}
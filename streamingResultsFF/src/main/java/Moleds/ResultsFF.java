package Moleds;

public class ResultsFF {
	public String date;
	public String home_team;
	public String away_team;
	public int home_score;
	public int away_score;
	public String tournament;
    public String city;
    public String country;
    public boolean neutral;
	
    
    public ResultsFF() {}

	public ResultsFF(String date, String home_team, String away_team, int home_score, int away_score, String tournament,
			String city, String country, boolean neutral) {
		super();
		this.date = date;
		this.home_team = home_team;
		this.away_team = away_team;
		this.home_score = home_score;
		this.away_score = away_score;
		this.tournament = tournament;
		this.city = city;
		this.country = country;
		this.neutral = neutral;
	}


	public String getDate() {
		return date;
	}


	public void setDate(String date) {
		this.date = date;
	}


	public String getHome_team() {
		return home_team;
	}


	public void setHome_team(String home_team) {
		this.home_team = home_team;
	}


	public String getAway_team() {
		return away_team;
	}


	public void setAway_team(String away_team) {
		this.away_team = away_team;
	}


	public int getHome_score() {
		return home_score;
	}


	public void setHome_score(int home_score) {
		this.home_score = home_score;
	}


	public int getAway_score() {
		return away_score;
	}


	public void setAway_score(int away_score) {
		this.away_score = away_score;
	}


	public String getTournament() {
		return tournament;
	}


	public void setTournament(String tournament) {
		this.tournament = tournament;
	}


	public String getCity() {
		return city;
	}


	public void setCity(String city) {
		this.city = city;
	}


	public String getCountry() {
		return country;
	}


	public void setCountry(String country) {
		this.country = country;
	}


	

	


	public boolean isNeutral() {
		return neutral;
	}


	public void setNeutral(boolean neutral) {
		this.neutral = neutral;
	}


	@Override
	public String toString() {
		return "Results [date=" + date + ", home_team=" + home_team + ", away_team=" + away_team + ", home_score="
				+ home_score + ", away_score=" + away_score + ", tournament=" + tournament + ", city=" + city
				+ ", country=" + country + ", neutral=" + neutral + "]";
	}

    

    
    
    
}
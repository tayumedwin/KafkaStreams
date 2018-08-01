package kafka.demo;

import java.io.Serializable;

public class AccountsBalance implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int id;
	private String currentBalance;
	private String availBalance;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getCurrentBalance() {
		return currentBalance;
	}
	public void setCurrentBalance(String currentBalance) {
		this.currentBalance = currentBalance;
	}
	public String getAvailBalance() {
		return availBalance;
	}
	public void setAvailBalance(String availBalance) {
		this.availBalance = availBalance;
	}
	
	

}

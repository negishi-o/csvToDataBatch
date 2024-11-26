package com.udemy.csvToDataBatch.model;

import java.util.Date;

import lombok.Data;

//↓↓このlombockアノテーションを利用する事で、ゲッター、セッターの定義は不要で自動で作成してくれる
@Data
public class Employee {

	private Integer empNumber;
	private String empName;
	private String jobTitle;
	private Integer mgrNumber;
	private Date hireDate;
}

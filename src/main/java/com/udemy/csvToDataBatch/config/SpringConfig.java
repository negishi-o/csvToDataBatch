package com.udemy.csvToDataBatch.config;

import java.nio.charset.StandardCharsets;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import com.udemy.csvToDataBatch.model.Employee;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class SpringConfig {
	private final JobLauncher jobLauncher;
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	
	
	@Value("${csv.path}")
	private Resource inputCSV;
	
	@Autowired
	private DataSource dataSource;
	
	private static final String INSERT_EMP_SQL =
			"INSERT INTO employee (empnumber, empname, jobtitle, mgrnumber, hiredate) "
			+ "VALUES(:empNumber, :empName, :jobTitle, :mgrNumber, :hireDate)";
	
	public SpringConfig(JobLauncher jobLauncher, JobRepository jobRepository,
			PlatformTransactionManager transactionManager) {
		this.jobLauncher = jobLauncher;
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
	}
	
	@Bean
	@StepScope
	public FlatFileItemReader<Employee> csvItemReader() {
		FlatFileItemReader<Employee> reader =
				new FlatFileItemReader<Employee>();
		
		reader.setResource(inputCSV);
		reader.setLinesToSkip(1);
		reader.setEncoding(StandardCharsets.UTF_8.name());

		//CSVと列とDBのカラムをマッピングするインスタンスここで、準備
		//まずは、対象テーブルをインスタンスにセット
		BeanWrapperFieldSetMapper<Employee> beanWrapperFiledSetMapper =
				new BeanWrapperFieldSetMapper<>();
		beanWrapperFiledSetMapper.setTargetType(Employee.class);
		
		//次に、CSVのタイトルを左から対象テーブルと同じカラム名としてセット
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		String[] csvTitleArray =
				new String[] {"EmpNumber", "EmpName", "JobTitle", "MgrNumber", "HireDate"};
		
		tokenizer.setNames(csvTitleArray);
		
		//準備したのをここで、CSVデータをテーブル情報としてぶち込める
		DefaultLineMapper<Employee> lineMapper =
				new DefaultLineMapper<Employee>();
		lineMapper.setFieldSetMapper(beanWrapperFiledSetMapper);
		lineMapper.setLineTokenizer(tokenizer);
		
		reader.setLineMapper(lineMapper);
		
		return reader;
	}
	
	@Autowired
	@Qualifier("EmpItemProcessor")
	public ItemProcessor<Employee, Employee> empItemProcessor;
	 
	@Bean
	@StepScope
	public JdbcBatchItemWriter<Employee> jdbcItemWriter(){
		
		
		//バインドしたSQLを利用する場合に、このインスタンスを利用する
		BeanPropertyItemSqlParameterSourceProvider<Employee> provider =
				new BeanPropertyItemSqlParameterSourceProvider<Employee>();
		
		JdbcBatchItemWriter<Employee> writer =
				new JdbcBatchItemWriter<Employee>();

		writer.setDataSource(dataSource);
		writer.setItemSqlParameterSourceProvider(provider);
		writer.setSql(INSERT_EMP_SQL);
		
		return writer;
	}
	
	@Bean
	public Step chunkStep() {
		return new StepBuilder("EmpItemProcessor", jobRepository)
				.<Employee, Employee>chunk(1, transactionManager)
				.reader(csvItemReader())
				.processor(empItemProcessor)
				.writer(jdbcItemWriter())
				.build();
				
	}
	
	@Bean
	public Job chunkJob() {
		return new JobBuilder("EmpItemJob", jobRepository)
				.incrementer(new RunIdIncrementer())//採番を付けて、バッチ処理の整合性を保つ
				.start(chunkStep())//Stepで作成したファンクションを呼び出す
				.build();
	}
	
}

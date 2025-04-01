INSERT INTO eligibility.configuration (organization_id, directory_name, data_provider) VALUES
	 (111, 'redbull', false);

INSERT INTO eligibility.file (id, organization_id, name, encoding ,started_at, completed_at) VALUES
	 (1234, 111, 'file.csv', 'utf8', '2022-04-21 15:56:03.57855-04', '2022-04-21 15:57:03.57855-04');

INSERT INTO eligibility.member (id, organization_id, file_id, first_name, last_name, email, unique_corp_id, dependent_id, date_of_birth, record, effective_range) VALUES
     -- File record
	 (111, 111, 1234, 'Daniel', 'Ricciardo', 'dricciardo@redbull.com', 'UC_ID_1', 'D_1', '2018-03-01', '{"unique_corp_id": "UC_ID_1", "dependent_id": "D_1"}', '[2020-01-02, infinity)'),
     -- Matching Kafka record
     (112, 111, NULL, 'Daniel', 'Ricciardo', 'dricciardo@redbull.com', 'S_ID_2', 'D_1', '2018-03-01', '{"altId": "UC_ID_1", "subscriberId": "S_ID_2", "primaryMemberId": "PM_ID_1", "random_field": "HEYY"}', '[2020-01-02, infinity)');

INSERT INTO eligibility.member_address (member_id,address_1,address_2,city,state,postal_code,postal_code_suffix,country_code,address_type) VALUES
	 (112, '123 Imola Lane', '435 Monza Ct', 'Cherry Hill', 'NJ', '21159', '53414', 'None', NULL);



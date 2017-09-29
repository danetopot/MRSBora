use etl;
drop procedure if exists generate_hiv_summary;
 DELIMITER $$
 CREATE PROCEDURE generate_hiv_summary()
 		BEGIN
        select @query_type := "sync"; # this can be either sync or rebuild
					select @start := now();
					select @start := now();
					select @table_version := "flat_hiv_summary_v2.13";

					set session sort_buffer_size=512000000;

					select @sep := " ## ";
					select @lab_encounter_type := 99999;
					select @death_encounter_type := 31;
					select @last_date_created := (select max(max_date_created) from etl.flat_obs);
                    create table if not exists flat_hiv_summary (
						person_id int,
						uuid varchar(100),
						visit_id int,
					    encounter_id int,
						encounter_datetime datetime,
						encounter_type int,
						is_clinical_encounter int,
						location_id int,
						location_uuid varchar(100),
						visit_num int,
						enrollment_date datetime,
						hiv_start_date datetime,
                        patient_source varchar(50),
                        cur_arv_adherence varchar(10),
                        primary key encounter_id (encounter_id),
                        index person_date (person_id, encounter_datetime),
						index person_uuid (uuid),
						index location_enc_date (location_uuid,encounter_datetime),
						index enc_date_location (encounter_datetime, location_uuid),
						index encounter_type (encounter_type)
					);
                    select @last_update := (select max(date_updated) from etl.flat_log where table_name=@table_version);
                   # then use the max_date_created from openmrs.encounter. This takes about 10 seconds and is better to avoid.
					select @last_update :=
						if(@last_update is null,
							(select max(date_created) from openmrs.encounter e join etl.flat_hiv_summary using (encounter_id)),
							@last_update);
                            #otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
					select @last_update := if(@last_update,@last_update,'1900-01-01');
					#select @last_update := "2016-09-12"; #date(now());
					#select @last_date_created := "2015-11-17"; #date(now());
                    	# drop table if exists flat_hiv_summary_queue;
					create  table if not exists flat_hiv_summary_queue(person_id int, primary key (person_id));
                    
                    # we will add new patient id to be rebuilt when either we  are in sync mode or if the existing table is empty
					# this will allow us to restart rebuilding the table if it crashes in the middle of a rebuild
					select @num_ids := (select count(*) from flat_hiv_summary_queue limit 1);

					if (@num_ids=0 or @query_type="sync") then
                        replace into flat_hiv_summary_queue
                        (select distinct patient_id #, min(encounter_datetime) as start_date
                            from openmrs.encounter
                            where date_changed > @last_update
                        );


                        replace into flat_hiv_summary_queue
                        (select distinct person_id #, min(encounter_datetime) as start_date
                            from etl.flat_obs
                            where max_date_created > @last_update
                        #	group by person_id
                        # limit 10
                        );
                      # Lab encountres
                      /*  replace into flat_hiv_summary_queue
                        (select distinct person_id
                            from etl.flat_lab_obs
                            where max_date_created > @last_update
                        ); */
                     # Lab Orders
                       /* replace into flat_hiv_summary_queue
                        (select distinct person_id
                            from etl.flat_orders
                            where max_date_created > @last_update
                        );
                        */
                         end if;
                         
                        select @person_ids_count := (select count(*) from flat_hiv_summary_queue);

				   delete t1 from flat_hiv_summary t1 join flat_hiv_summary_queue t2 using (person_id);

					while @person_ids_count > 0 do

						#create temp table with a set of person ids
						drop table if exists flat_hiv_summary_queue_0;

						create temporary table flat_hiv_summary_queue_0 (select * from flat_hiv_summary_queue limit 5000); #TODO - change this when data_fetch_size changes


						select @person_ids_count := (select count(*) from flat_hiv_summary_queue);

						drop table if exists flat_hiv_summary_0a;
						create temporary table flat_hiv_summary_0a
						(select
							t1.person_id,
							t1.visit_id,
							t1.encounter_id,
							t1.encounter_datetime,
							t1.encounter_type,
							t1.location_id,
							t1.obs,
							t1.obs_datetimes,
                            
                            case
								when t1.encounter_type in (21,22) then 1
								else null
							end as is_clinical_encounter,
                            case
						        when t1.encounter_type in (35) then 20
								when t1.encounter_type in (45) then 10
								else 1
							end as encounter_type_sort_index,
                            t2.orders
							from etl.flat_obs t1
								join flat_hiv_summary_queue_0 t0 using (person_id)
								left join etl.flat_orders t2 using(encounter_id)
						#		join flat_hiv_summary_queue t0 on t1.person_id=t0.person_id and t1.encounter_datetime >= t0.start_date
							where t1.encounter_type in (21,22)
						);
                        insert into flat_hiv_summary_0a
						(select
							t1.person_id,
							null,
							t1.encounter_id,
							t1.test_datetime,
							t1.encounter_type,
							t1.location_id, # null, ,
							t1.obs,
							null, #obs_datetimes
							# in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
							0 as is_clinical_encounter,
							1 as encounter_type_sort_index,
							null
							from etl.flat_lab_obs t1
								join flat_hiv_summary_queue_0 t0 using (person_id)
						);
                        
                        drop table if exists flat_hiv_summary_0;
						create temporary table flat_hiv_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_hiv_summary_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                        
                       
						select @prev_id := null;
						select @cur_id := null;
                        select @enrollment_date := null;
						select @hiv_start_date := null;
                        drop temporary table if exists flat_hiv_summary_1;
						create temporary table flat_hiv_summary_1 (index encounter_id (encounter_id))
						(select
							encounter_type_sort_index,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
							t1.person_id,
							p.uuid,
							t1.visit_id,
							t1.encounter_id,
							t1.encounter_datetime,
							t1.encounter_type,
							t1.is_clinical_encounter,
							t1.location_id,
							case
								when obs regexp "!!6746=" then @enrollment_date :=
									replace(replace((substring_index(substring(obs,locate("!!6746=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!6746=", "") ) ) / LENGTH("!!6746=") ))),"!!6746=",""),"!!","")
								else @enrollment_date:= null
							end as enrollment_date,
                            #patient source
                            case
								 when obs regexp "!!1353=(1356)!!" then @patient_source:="PMTCT"
								 when obs regexp "!!1353=(1354)!!" then @patient_source:="VCT"
                                 when obs regexp "!!1353=(6767)!!" then @patient_source:="IPD-Ad"
								 when obs regexp "!!1353=(1360)!!" then @patient_source:="TB Clinic"
                                 when obs regexp "!!1353=(1357)!!" then @patient_source:="OPD"
								 when obs regexp "!!1353=(6768)!!" then @patient_source:="IPD-Ch"
                                 when obs regexp "!!1353=(1358)!!" then @patient_source:="MCH-Child"
								 when obs regexp "!!1353=(1828)!!" then @patient_source:="VMMC"
                                 when obs regexp "!!1353=(1355)!!" then @patient_source:="Family Member"
                                 when obs regexp "!!1353=(5622)!!" then @patient_source:="Other"
                                 else @patient_source:= replace(replace((substring_index(substring(obs,locate("!!1353=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!1353=", "") ) ) / LENGTH("!!1353=") ))),"!!1353=",""),"!!","")

							end as patient_source,
                             
                            # 6760 ART ADHERENCE
                            # 1384 = GOOD
							# 1385 = FAIR
							# 1386 = POOR
                            # 1175 = N/A
							case
								when obs regexp "!!6760=1384!!" then @cur_arv_adherence := 'GOOD'
								when obs regexp "!!6760=1385!!" then @cur_arv_adherence := 'FAIR'
								when obs regexp "!!6760=1386!!" then @cur_arv_adherence := 'POOR'
                                when obs regexp "!!6760=1175!!" then @cur_arv_adherence := 'N/A'
								else @cur_arv_adherence := null
							end as cur_arv_adherence
                            
                            from flat_hiv_summary_0 t1 
							join openmrs.person p using (person_id)
                            where encounter_type in (21,22)
						 
                            );
                            
                            replace into flat_hiv_summary
							(select
                            f1.person_id,
                            f1.uuid,
                            f1.visit_id,
                            f1.encounter_id,
                            f1.encounter_datetime,
							f1.encounter_type,
							f1.is_clinical_encounter,
							f1.location_id,
                            2,3,
                            f1.enrollment_date,
                            f1.encounter_datetime,
                            f1.patient_source,
                            f1.cur_arv_adherence
						  	from flat_hiv_summary_1 f1
							);
						
					
					  end while;
                      
                      END $$
	DELIMITER ;

call generate_hiv_summary();
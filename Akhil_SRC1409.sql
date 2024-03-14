create or replace PACKAGE BODY PKG_NY_MIG_SRC_EXTRACTION_LOAD IS

    -- Regrade changes. Added  parameter V_BILLINGACCOUNTNUMBER,V_REGRADE_TYPE for N 2 N Regrade
PROCEDURE PRC_NAYAN_MIGRATION_LOAD(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER,V_START_TIME IN NUMBER, 
V_BILLINGACCOUNTNUMBER IN VARCHAR2 ,V_REGRADE_TYPE IN VARCHAR2) AS
    V_SRC_START_TIME NUMBER;
    V_SRC_END_TIME  NUMBER;
    V_END_TIME      NUMBER;
    v_log_message   VARCHAR2(4000);
    V_NEW_ORDER_CNT NUMBER := 0;
    V_CNT           NUMBER := 0;
	V_DP_CNT        NUMBER := 0;
	V_OV_CNT        NUMBER := 0;
	V_VL_CNT        NUMBER := 0;
    V_OV_FAIL_CNT   NUMBER := 0;
    V_ORD_BB_CNT    NUMBER := 0;
    V_XMLREQ_CNT    NUMBER := 0;
    V_ORDER_STATUS  VARCHAR2(100);
    V_ERR_LOG_MSG   VARCHAR2(1000);
    V_error_code    NUMBER;
    V_ERR_RTN_DETAILS VARCHAR2(1000);
    V_ERR_RTN_PRODUCTS VARCHAR2(1000);
  	-- REGRADE CHANGES START 
	V_BILLINGACC_EXISTS VARCHAR2(1);
	V_FAILURE_FLAG VARCHAR2(1);
	V_IS_REGRADE VARCHAR2(1) := 'N';
	-- REGRADE CHANGES end 
    V_DISCONNECT_COUNT NUMBER := 0; -- Cease journey changes


BEGIN
V_SRC_START_TIME := dbms_utility.get_time;
PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Loading source tables and generating request order xml','PRC_NAYAN_MIGRATION_LOAD','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_NEW_ORDER_CNT
        FROM
            JSON_ETL_PAYLOAD
        WHERE
            FILE_ID = V_FILE_ID
			AND STATUS = 'NEW';

IF V_NEW_ORDER_CNT > 0 THEN
-- Regarde Changes start 

      PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,V_BILLINGACCOUNTNUMBER, 'Loading source tables and generating TMF File for Regrade Scenario','PRC_NAYAN_MIGRATION_LOAD','Procedure - start','NEW', v_log_message, SYSDATE );

	GET_TMF_OV_RESPONSE (v_NY_file_id => v_file_id,
										v_ordernumber => v_ordernumber,
										V_BILLINGACCOUNTNUMBER => V_BILLINGACCOUNTNUMBER,
										 in_wallet   => 'TMF_OV_WALLET',     
										 in_url        => 'TMF_OV_URL',
--										in_file_type  => 'BILLINGACCOUNTNO',
										in_password   => 'TMF_OV_PASSWORD',
										V_REGRADE_TYPE => V_REGRADE_TYPE,
										out_error_message =>V_ERR_LOG_MSG,
										out_error_code   => V_error_code);


        BEGIN
                SELECT 
                    BILLINGACC_EXISTS, FAILURE_FLAG 
                INTO 
                    V_BILLINGACC_EXISTS, V_FAILURE_FLAG
                FROM 
                    TMF_OV_RESPONSE_TABLE
                WHERE 
                    ORDERNUMBER = V_ORDERNUMBER;

        EXCEPTION
            WHEN NO_DATA_FOUND 
                THEN 
               V_BILLINGACC_EXISTS := 'N'; 
               V_FAILURE_FLAG := 'Y';
        END;

			IF V_FAILURE_FLAG =  'N' 
				THEN 


				IF V_BILLINGACC_EXISTS = 'Y'
				THEN 

                UPDATE JSON_ETL_PAYLOAD
                SET IS_REGRADE = 'Y'
                WHERE
				    FILE_ID = V_FILE_ID;

                    V_IS_REGRADE := 'Y';


					PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_TMF_orderproduct(v_ordernumber,v_file_id);

					PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_TMF_tmf_productcharacteristic(v_ordernumber,v_file_id);

					PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_TMF_productinstanceids(v_ordernumber,v_file_id);

					PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_TMF_relatedparty(v_ordernumber,v_file_id);

					PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_TMF_place(v_ordernumber,v_file_id);			

                END IF;						 
		    END IF;


-- REGRAGE changes end 

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_BILLING_ACCOUNT(v_ordernumber,v_file_id);

	PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_ORDER_DETAILS(v_ordernumber,v_file_id);

     PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_METADATA_ATTRIBUTES(v_ordernumber,v_file_id);

  /*ERROR HANDLING CHANGES DONE BY RUDHRA KUMAR NAGARAJAN FOR ORDER DETAILS ON 16/06/22* - STARTS*/


SELECT 
   COUNT(1)          
INTO
   V_ERR_RTN_DETAILS  
FROM 
   NY_MIG_ERROR_LOG
WHERE 
   FILE_ID = V_FILE_ID
AND
   LOG_TYPE = 'JSON_FILE_ERROR_LOG' ;


IF 
   V_ERR_RTN_DETAILS > 0 THEN

   PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Loading source tables and generating request order xml','PRC_NAYAN_MIGRATION_LOAD1','Procedure - ends cause of no order details records found','NEW', v_log_message, SYSDATE );

    V_ERR_LOG_MSG := 'Order details does not exists for the given Ordernumber => '|| V_ORDERNUMBER;

    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD1', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

     POST_VELOCITY_CALLBACK_API(v_file_id => v_file_id,
                                v_ordernumber => v_ordernumber,
                                 in_wallet   => 'VLOCITY_WALLET',     
                                 in_url      => 'VLOCITY_URL',
                                in_password  => 'VLOCITY_PASSWORD',
                                out_error_message =>V_ERR_LOG_MSG,
                                 out_error_code   => V_error_code);   

RETURN;

ELSIF

V_ERR_RTN_DETAILS = 0 THEN


	PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_ORDER_ATTRIBUTES(v_ordernumber,v_file_id);

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_DISCOUNT_ATTRIBUTES(v_ordernumber,v_file_id);

	PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_ORDER_PRODUCT(v_ordernumber,v_file_id);

/*ERROR HANDLING CHANGES DONE BY RUDHRA KUMAR NAGARAJAN FOR ORDER PRODUCTS ON 16/06/22* - STARTS*/

/* Validating the appointment section for the given ordernumber */
For r in (SELECT count(case when appointment_reference__c = 'true' then 1 end) as Appointment_cnt
    From
        (SELECT appointment_reference__c,ProductName
            FROM JSON_SOURCE_INPUT_FILE a,
            JSON_TABLE ( INPUT_FILE, '$[*]'
    COLUMNS (
        NESTED PATH '$.OrderProduct[*]'
                            COLUMNS (
                        appointment_reference__c VARCHAR ( 100 ) EXISTS PATH '$.Appointment.Appointment_Reference__c',
                        ProductName VARCHAR ( 100 ) PATH '$.ProductName'
                    )
                )
                )
            X
WHERE a.FILE_ID = V_FILE_ID
and (x.PRODUCTNAME ='Managed Install' OR x.PRODUCTNAME ='Broadband Services' OR x.PRODUCTNAME ='Self Install'))
) loop


/* handled appointment section is missing for the given ordernumber */
    IF r.Appointment_cnt = 1 THEN

        PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_CHECK_APPOINTMENT_ATTR(v_ordernumber,v_file_id);

                 SELECT 
                 COUNT(*)  
                 INTO V_ORD_BB_CNT
                 FROM NY_MIG_ORDER_PRODUCT 
                 WHERE FILE_ID = V_FILE_ID
                 AND APPOINTMENT_REFERENCE__C IS NOT NULL;

                            IF V_ORD_BB_CNT = 0 THEN

                                PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'APPOINTMENT REF_C IS NULL','PRC_NAYAN_MIGRATION_LOAD','Procedure - ends cause of no order products records found','NEW', v_log_message, SYSDATE );

                                V_ERR_LOG_MSG := 'APPOINTMENT_REFERENCE__C IS NULL FOR THE GIVEN ORDERNUMBER => '|| V_ORDERNUMBER;

                                PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

                                POST_VELOCITY_CALLBACK_API(v_file_id => v_file_id,
                                        v_ordernumber => v_ordernumber,
                                        in_wallet   => 'VLOCITY_WALLET',     
                                        in_url      => 'VLOCITY_URL',
                                        in_password  => 'VLOCITY_PASSWORD',
                                        out_error_message =>V_ERR_LOG_MSG,
                                        out_error_code   => V_error_code);

                                RETURN;
                            END IF;
     ELSIF r.Appointment_cnt > 1 THEN

			V_ERR_LOG_MSG := 'Retrieving the APPOINTMENT section for more than one products for the given ordernumber => '|| V_ORDERNUMBER;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

			POST_VELOCITY_CALLBACK_API(v_file_id => v_file_id,
                                v_ordernumber => v_ordernumber,
                                 in_wallet   => 'VLOCITY_WALLET',     
                                 in_url      => 'VLOCITY_URL',
                                in_password  => 'VLOCITY_PASSWORD',
                                out_error_message =>V_ERR_LOG_MSG,
                                 out_error_code   => V_error_code);
            RETURN;
/*
     ELSIF r.Appointment_cnt = 0 THEN

			V_ERR_LOG_MSG := 'APPOINTMENT Attribute is missing for the given ordernumber => '|| V_ORDERNUMBER;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

			POST_VELOCITY_CALLBACK_API(v_file_id => v_file_id,
                                v_ordernumber => v_ordernumber,
                                 in_wallet   => 'VLOCITY_WALLET',     
                                 in_url      => 'VLOCITY_URL',
                                in_password  => 'VLOCITY_PASSWORD',
                                out_error_message =>V_ERR_LOG_MSG,
                                 out_error_code   => V_error_code);
*/
     END IF;

END LOOP;

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_ORDER_PAYMENT(v_ordernumber,v_file_id);

	PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_CT_PT_ADDRESS(v_ordernumber,v_file_id);

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_CPA_GEOMETRY(v_ordernumber,v_file_id);

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_CPA_GEO_SUBADDRESS(v_ordernumber,v_file_id);

	PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_TECH_ORDER_DETAIL(v_ordernumber,v_file_id);

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_TOD_GEOMETRY(v_ordernumber,v_file_id);

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_TOD_GEO_SUBADDRESS(v_ordernumber,v_file_id);

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_TOD_SUPPLIEROFFERS(v_ordernumber,v_file_id);

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_TOD_ONTDETAILS(v_ordernumber,v_file_id);

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_TOD_ONTDETAILS_PORT(v_ordernumber,v_file_id);

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_TOD_RELATED_PARTY(v_ordernumber,v_file_id);

	PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_VCODE_SCODE_MAP(v_ordernumber,v_file_id);

    -- Regrade changes. Added parameter V_IS_REGRADE
    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_PRODUCT_TREE_UPDATE(v_ordernumber,v_file_id, V_IS_REGRADE,V_REGRADE_TYPE);

    -- Regrade changes. Added parameter V_IS_REGRADE
    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_PRODUCT_TYPE_LINK(v_ordernumber,v_file_id, V_IS_REGRADE,V_REGRADE_TYPE);

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_TOD_WORKING_LINES(v_ordernumber,v_file_id);

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_SRC_TOD_STOPPED_LINES(v_ordernumber,v_file_id);

     IF V_IS_REGRADE = 'Y' THEN 

         PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_TMF_UPDATE_PRODUCT_TREE(v_ordernumber,v_file_id,V_REGRADE_TYPE);

      END IF;

    PKG_NY_MIG_SRC_EXTRACTION_LOAD.PRC_XML_INSTANCECHARACTERSTICLIST(v_ordernumber,v_file_id ,V_IS_REGRADE ,V_REGRADE_TYPE); /* Added for None and Ammend scenario by Bala/kanagavel/Ranganath on 08-08-2022*/

END IF;

  /*ERROR HANDLING CHANGES DONE BY RUDHRA KUMAR NAGARAJAN FOR ORDER DETAILS ON 16/06/22 - ENDS*/

  /*ERROR HANDLING CHANGES DONE BY RUDHRA KUMAR NAGARAJAN FOR ORDER PRODUCTS ON 16/06/22 - ENDS*/

/* E2E Code Getting response from OV DP and Vlocity - Starts*/
  SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_PRODUCT_TREE
        WHERE
            FILE_ID = V_FILE_ID
			AND STATUS = 'NEW'
            AND ((PRODUCTCODE IS NOT NULL and upper(ov_action) <> 'DELETE' )
                OR (upper(ov_action) = 'DELETE')) ; -- CEASE Journey changes

        IF V_CNT > 0 THEN

            V_SRC_END_TIME := dbms_utility.get_time;

            INSERT INTO NY_MIG_PROCESSSTEP_TABLE
            (FILE_ID,
            ORDERNUMBER,
            STATUS,
            PROCESSSTEP,
            CREATED_DT,
            STARTTIME,
            ENDTIME,
            ELAPSEDTIME)
            VALUES(v_file_id,
            V_ORDERNUMBER,
            'Success',
            'Validation',
            SYSTIMESTAMP,
            V_SRC_START_TIME,
            V_SRC_END_TIME,
            ROUND((V_SRC_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
            );

            COMMIT;


        --dbms_output.put_Line ('Source extraction Elapsed Time = '||ROUND((V_SRC_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds');

            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Loading source tables and generating request order xml','PRC_NAYAN_MIGRATION_LOAD','Source extraction Elapsed Time = '||ROUND((V_SRC_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT = 0 THEN

            V_SRC_END_TIME := dbms_utility.get_time;

            INSERT INTO NY_MIG_PROCESSSTEP_TABLE
            (FILE_ID,
            ORDERNUMBER,
            STATUS,
            PROCESSSTEP,
            CREATED_DT,
            STARTTIME,
            ENDTIME,
            ELAPSEDTIME)
            VALUES(v_file_id,
            V_ORDERNUMBER,
            'Failure',
            'Validation',
            SYSTIMESTAMP,
            V_SRC_START_TIME,
            V_SRC_END_TIME,
            ROUND((V_SRC_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
            );

            COMMIT;

            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Loading source tables and generating request order xml','PRC_NAYAN_MIGRATION_LOAD','Source extraction Elapsed Time = '||ROUND((V_SRC_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

            V_ERR_LOG_MSG := 'Order Items does not exists for the given Ordernumber => '|| V_ORDERNUMBER;
            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

            POST_VELOCITY_CALLBACK_API(v_file_id => v_file_id,
                                v_ordernumber => v_ordernumber,
                                 in_wallet   => 'VLOCITY_WALLET',     
                                 in_url      => 'VLOCITY_URL',
                                in_password  => 'VLOCITY_PASSWORD',
                                out_error_message =>V_ERR_LOG_MSG,
                                 out_error_code   => V_error_code);

                SELECT
                    COUNT(1)
                INTO V_VL_CNT
                FROM
                    XML_RESPONSE_TABLE
                WHERE
                    ORDERNUMBER = V_ORDERNUMBER
                    AND XML_TYPE = 'VLOCITY_RESPONSE'
                    AND FAILURE_FLAG = 'N';

                IF V_VL_CNT > 0 THEN

                    V_END_TIME := dbms_utility.get_time;

                    INSERT INTO NY_MIG_PROCESSSTEP_TABLE
                    (FILE_ID,
                    ORDERNUMBER,
                    STATUS,
                    PROCESSSTEP,
                    CREATED_DT,
                    STARTTIME,
                    ENDTIME,
                    ELAPSEDTIME)
                    VALUES(v_file_id,
                    V_ORDERNUMBER,
                    'Success',
                    'Json Response to Vlocity',
                    SYSTIMESTAMP,
                    V_SRC_START_TIME,
                    V_END_TIME,
                    ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
                    );

                    COMMIT;

                ELSE

                    V_END_TIME := dbms_utility.get_time;

                    INSERT INTO NY_MIG_PROCESSSTEP_TABLE
                    (FILE_ID,
                    ORDERNUMBER,
                    STATUS,
                    PROCESSSTEP,
                    CREATED_DT,
                    STARTTIME,
                    ENDTIME,
                    ELAPSEDTIME)
                    VALUES(v_file_id,
                    V_ORDERNUMBER,
                    'Failure',
                    'Json Response to Vlocity',
                    SYSTIMESTAMP,
                    V_SRC_START_TIME,
                    V_END_TIME,
                    ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
                    );

                    COMMIT;

                    V_ERR_LOG_MSG := 'Json Response to Vlocity is failed for the given Ordernumber => '|| V_ORDERNUMBER;
                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                END IF;

			RETURN;

        END IF;

            PKG_NY_MIG_XML_GENERATION.PRC_XML_GENERATION(v_ordernumber,v_file_id,V_SRC_START_TIME, V_REGRADE_TYPE,V_IS_REGRADE);

            SELECT
                COUNT(1)
            INTO V_XMLREQ_CNT
            FROM
                XML_LOAD_TABLE
            WHERE
                ORDERNUMBER = V_ORDERNUMBER;

            IF V_XMLREQ_CNT = 0 THEN

                V_END_TIME := dbms_utility.get_time;

                INSERT INTO NY_MIG_PROCESSSTEP_TABLE
                (FILE_ID,
                ORDERNUMBER,
                STATUS,
                PROCESSSTEP,
                CREATED_DT,
                STARTTIME,
                ENDTIME,
                ELAPSEDTIME)
                VALUES(v_file_id,
                V_ORDERNUMBER,
                'Failure',
                'Data Transformation to CCP model',
                SYSTIMESTAMP,
                V_SRC_START_TIME,
                V_END_TIME,
                ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
                );

                COMMIT;

            V_ERR_LOG_MSG := 'Data Transformation to CCP model is failed for the given Ordernumber => '|| V_ORDERNUMBER;
            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

            POST_VELOCITY_CALLBACK_API(v_file_id => v_file_id,
                                v_ordernumber => v_ordernumber,
                                 in_wallet   => 'VLOCITY_WALLET',     
                                 in_url      => 'VLOCITY_URL',
                                in_password  => 'VLOCITY_PASSWORD',
                                out_error_message =>V_ERR_LOG_MSG,
                                 out_error_code   => V_error_code);

                SELECT
                    COUNT(1)
                INTO V_VL_CNT
                FROM
                    XML_RESPONSE_TABLE
                WHERE
                    ORDERNUMBER = V_ORDERNUMBER
                    AND XML_TYPE = 'VLOCITY_RESPONSE'
                    AND FAILURE_FLAG = 'N';

                IF V_VL_CNT > 0 THEN

                    V_END_TIME := dbms_utility.get_time;

                    INSERT INTO NY_MIG_PROCESSSTEP_TABLE
                    (FILE_ID,
                    ORDERNUMBER,
                    STATUS,
                    PROCESSSTEP,
                    CREATED_DT,
                    STARTTIME,
                    ENDTIME,
                    ELAPSEDTIME)
                    VALUES(v_file_id,
                    V_ORDERNUMBER,
                    'Success',
                    'Json Response to Vlocity',
                    SYSTIMESTAMP,
                    V_SRC_START_TIME,
                    V_END_TIME,
                    ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
                    );

                    COMMIT;

                ELSE

                    V_END_TIME := dbms_utility.get_time;

                    INSERT INTO NY_MIG_PROCESSSTEP_TABLE
                    (FILE_ID,
                    ORDERNUMBER,
                    STATUS,
                    PROCESSSTEP,
                    CREATED_DT,
                    STARTTIME,
                    ENDTIME,
                    ELAPSEDTIME)
                    VALUES(v_file_id,
                    V_ORDERNUMBER,
                    'Failure',
                    'Json Response to Vlocity',
                    SYSTIMESTAMP,
                    V_SRC_START_TIME,
                    V_END_TIME,
                    ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
                    );

                    COMMIT;

                    V_ERR_LOG_MSG := 'Json Response to Vlocity is failed for the given Ordernumber => '|| V_ORDERNUMBER;
                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                END IF;

				RETURN;

            END IF;

            --UTL_HTTP.set_wallet(v_wallet,v_password);     
            post_ov_orders_test(v_file_id => v_file_id,
                                v_ordernumber => v_ordernumber,
                                 in_wallet   => 'DATAPOWER_WALLET',     
                                 in_url        => 'DATAPOWER_URL',
                                 in_file_type  => 'xml',
                                in_password   => 'DATAPOWER_PASSWORD',
                                out_error_message =>V_ERR_LOG_MSG,
                                 out_error_code   => V_error_code);

        SELECT
            COUNT(1)
        INTO V_DP_CNT
        FROM
            XML_RESPONSE_TABLE
        WHERE
            ORDERNUMBER = V_ORDERNUMBER
			AND XML_TYPE = 'RESPONSE'
			AND FAILURE_FLAG = 'N';

        IF V_DP_CNT > 0 THEN
            V_END_TIME := dbms_utility.get_time;

            INSERT INTO NY_MIG_PROCESSSTEP_TABLE
            (FILE_ID,
            ORDERNUMBER,
            STATUS,
            PROCESSSTEP,
            CREATED_DT,
            STARTTIME,
            ENDTIME,
            ELAPSEDTIME)
            VALUES(v_file_id,
            V_ORDERNUMBER,
            'Success',
            'Posted to DataPower',
            SYSTIMESTAMP,
            V_SRC_START_TIME,
            V_END_TIME,
            ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
            );

            COMMIT;

		  GET_OV_ORDERS_RESPONSE(
			V_FILE_ID => v_file_id,
			V_ORDERNUMBER => v_ordernumber,
			IN_WALLET => 'ONEVIEW_WALLET',
			IN_URL => 'ONEVIEW_URL',
			IN_PASSWORD => 'ONEVIEW_PASSWORD',
			OUT_ERROR_MESSAGE => V_ERR_LOG_MSG,
			OUT_ERROR_CODE => V_error_code
		  );

        ELSE

            V_END_TIME := dbms_utility.get_time;

            INSERT INTO NY_MIG_PROCESSSTEP_TABLE
            (FILE_ID,
            ORDERNUMBER,
            STATUS,
            PROCESSSTEP,
            CREATED_DT,
            STARTTIME,
            ENDTIME,
            ELAPSEDTIME)
            VALUES(v_file_id,
            V_ORDERNUMBER,
            'Failure',
            'Posted to DataPower',
            SYSTIMESTAMP,
            V_SRC_START_TIME,
            V_END_TIME,
            ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
            );

            COMMIT;

            V_ERR_LOG_MSG := 'Datapower response does not exists for the given Ordernumber => '|| V_ORDERNUMBER;
            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

            POST_VELOCITY_CALLBACK_API(v_file_id => v_file_id,
                                v_ordernumber => v_ordernumber,
                                 in_wallet   => 'VLOCITY_WALLET',     
                                 in_url      => 'VLOCITY_URL',
                                in_password  => 'VLOCITY_PASSWORD',
                                out_error_message =>V_ERR_LOG_MSG,
                                 out_error_code   => V_error_code);

                SELECT
                    COUNT(1)
                INTO V_VL_CNT
                FROM
                    XML_RESPONSE_TABLE
                WHERE
                    ORDERNUMBER = V_ORDERNUMBER
                    AND XML_TYPE = 'VLOCITY_RESPONSE'
                    AND FAILURE_FLAG = 'N';

                IF V_VL_CNT > 0 THEN

                    V_END_TIME := dbms_utility.get_time;

                    INSERT INTO NY_MIG_PROCESSSTEP_TABLE
                    (FILE_ID,
                    ORDERNUMBER,
                    STATUS,
                    PROCESSSTEP,
                    CREATED_DT,
                    STARTTIME,
                    ENDTIME,
                    ELAPSEDTIME)
                    VALUES(v_file_id,
                    V_ORDERNUMBER,
                    'Success',
                    'Json Response to Vlocity',
                    SYSTIMESTAMP,
                    V_SRC_START_TIME,
                    V_END_TIME,
                    ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
                    );

                    COMMIT;

                ELSE

                    V_END_TIME := dbms_utility.get_time;

                    INSERT INTO NY_MIG_PROCESSSTEP_TABLE
                    (FILE_ID,
                    ORDERNUMBER,
                    STATUS,
                    PROCESSSTEP,
                    CREATED_DT,
                    STARTTIME,
                    ENDTIME,
                    ELAPSEDTIME)
                    VALUES(v_file_id,
                    V_ORDERNUMBER,
                    'Failure',
                    'Json Response to Vlocity',
                    SYSTIMESTAMP,
                    V_SRC_START_TIME,
                    V_END_TIME,
                    ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
                    );

                    COMMIT;

                    V_ERR_LOG_MSG := 'Json Response to Vlocity is failed for the given Ordernumber => '|| V_ORDERNUMBER;
                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                END IF;

                RETURN;

        END IF;

/* Added by Elen on 14-June-2022 as part of C0 Retry to get One View Response - Starts*/

        SELECT
            COUNT(1)
        INTO V_OV_CNT
        FROM
            XML_RESPONSE_TABLE
        WHERE
            ORDERNUMBER = V_ORDERNUMBER
			AND XML_TYPE = 'ONEVIEW_RESPONSE'
			AND FAILURE_FLAG = 'N';

        IF V_OV_CNT > 0 THEN

            V_END_TIME := dbms_utility.get_time;

            INSERT INTO NY_MIG_PROCESSSTEP_TABLE
            (FILE_ID,
            ORDERNUMBER,
            STATUS,
            PROCESSSTEP,
            CREATED_DT,
            STARTTIME,
            ENDTIME,
            ELAPSEDTIME)
            VALUES(v_file_id,
            V_ORDERNUMBER,
            'Success',
            'Order Submitted in OneView',
            SYSTIMESTAMP,
            V_SRC_START_TIME,
            V_END_TIME,
            ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
            );

            COMMIT;

                POST_VELOCITY_CALLBACK_API(v_file_id => v_file_id,
                                    v_ordernumber => v_ordernumber,
                                     in_wallet   => 'VLOCITY_WALLET',     
                                     in_url      => 'VLOCITY_URL',
                                    in_password  => 'VLOCITY_PASSWORD',
                                    out_error_message =>V_ERR_LOG_MSG,
                                     out_error_code   => V_error_code);

        END IF;

        SELECT
            COUNT(1)
        INTO V_OV_FAIL_CNT
        FROM
            XML_RESPONSE_TABLE
        WHERE
            ORDERNUMBER = V_ORDERNUMBER
			AND XML_TYPE = 'ONEVIEW_RESPONSE'
			AND FAILURE_FLAG = 'Y';

        IF V_OV_FAIL_CNT > 0 THEN

            V_END_TIME := dbms_utility.get_time;

            INSERT INTO NY_MIG_PROCESSSTEP_TABLE
            (FILE_ID,
            ORDERNUMBER,
            STATUS,
            PROCESSSTEP,
            CREATED_DT,
            STARTTIME,
            ENDTIME,
            ELAPSEDTIME)
            VALUES(v_file_id,
            V_ORDERNUMBER,
            'Failure',
            'Order Submitted in OneView',
            SYSTIMESTAMP,
            V_SRC_START_TIME,
            V_END_TIME,
            ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
            );

            COMMIT;

		  GET_OV_ORDERS_RESPONSE(
			V_FILE_ID => v_file_id,
			V_ORDERNUMBER => v_ordernumber,
			IN_WALLET => 'ONEVIEW_WALLET',
			IN_URL => 'ONEVIEW_URL',
			IN_PASSWORD => 'ONEVIEW_PASSWORD',
			OUT_ERROR_MESSAGE => V_ERR_LOG_MSG,
			OUT_ERROR_CODE => V_error_code
		  );

            SELECT
                COUNT(1)
            INTO V_OV_CNT
            FROM
                XML_RESPONSE_TABLE
            WHERE
                ORDERNUMBER = V_ORDERNUMBER
                AND XML_TYPE = 'ONEVIEW_RESPONSE'
                AND FAILURE_FLAG = 'N';

            IF V_OV_CNT > 0 THEN

                V_END_TIME := dbms_utility.get_time;

                INSERT INTO NY_MIG_PROCESSSTEP_TABLE
                (FILE_ID,
                ORDERNUMBER,
                STATUS,
                PROCESSSTEP,
                CREATED_DT,
                STARTTIME,
                ENDTIME,
                ELAPSEDTIME)
                VALUES(v_file_id,
                V_ORDERNUMBER,
                'Success',
                'Retry to Submit Order in OneView',
                SYSTIMESTAMP,
                V_SRC_START_TIME,
                V_END_TIME,
                ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
                );

                COMMIT;

                POST_VELOCITY_CALLBACK_API(v_file_id => v_file_id,
                                    v_ordernumber => v_ordernumber,
                                     in_wallet   => 'VLOCITY_WALLET',     
                                     in_url      => 'VLOCITY_URL',
                                    in_password  => 'VLOCITY_PASSWORD',
                                    out_error_message =>V_ERR_LOG_MSG,
                                     out_error_code   => V_error_code);

            ELSE

                V_END_TIME := dbms_utility.get_time;

                INSERT INTO NY_MIG_PROCESSSTEP_TABLE
                (FILE_ID,
                ORDERNUMBER,
                STATUS,
                PROCESSSTEP,
                CREATED_DT,
                STARTTIME,
                ENDTIME,
                ELAPSEDTIME)
                VALUES(v_file_id,
                V_ORDERNUMBER,
                'Failure',
                'Retry to Submit Order in OneView',
                SYSTIMESTAMP,
                V_SRC_START_TIME,
                V_END_TIME,
                ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
                );

                COMMIT;

                V_ERR_LOG_MSG := 'One View response does not received for the given Ordernumber => '|| V_ORDERNUMBER;
                PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

                POST_VELOCITY_CALLBACK_API(v_file_id => v_file_id,
                                    v_ordernumber => v_ordernumber,
                                     in_wallet   => 'VLOCITY_WALLET',     
                                     in_url      => 'VLOCITY_URL',
                                    in_password  => 'VLOCITY_PASSWORD',
                                    out_error_message =>V_ERR_LOG_MSG,
                                     out_error_code   => V_error_code);

            END IF;

		END IF;
/* Added by Elen on 14-June-2022 as part of C0 Retry to get One View Response - ends*/


        SELECT
            COUNT(1)
        INTO V_VL_CNT
        FROM
            XML_RESPONSE_TABLE
        WHERE
            ORDERNUMBER = V_ORDERNUMBER
			AND XML_TYPE = 'VLOCITY_RESPONSE'
			AND FAILURE_FLAG = 'N';

        IF V_VL_CNT > 0 THEN

            V_END_TIME := dbms_utility.get_time;

            INSERT INTO NY_MIG_PROCESSSTEP_TABLE
            (FILE_ID,
            ORDERNUMBER,
            STATUS,
            PROCESSSTEP,
            CREATED_DT,
            STARTTIME,
            ENDTIME,
            ELAPSEDTIME)
            VALUES(v_file_id,
            V_ORDERNUMBER,
            'Success',
            'Json Response to Vlocity',
            SYSTIMESTAMP,
            V_SRC_START_TIME,
            V_END_TIME,
            ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
            );

            COMMIT;

        ELSE

            V_END_TIME := dbms_utility.get_time;

            INSERT INTO NY_MIG_PROCESSSTEP_TABLE
            (FILE_ID,
            ORDERNUMBER,
            STATUS,
            PROCESSSTEP,
            CREATED_DT,
            STARTTIME,
            ENDTIME,
            ELAPSEDTIME)
            VALUES(v_file_id,
            V_ORDERNUMBER,
            'Failure',
            'Json Response to Vlocity',
            SYSTIMESTAMP,
            V_SRC_START_TIME,
            V_END_TIME,
            ROUND((V_END_TIME - V_SRC_START_TIME)/100,2)||' Seconds'
            );

            COMMIT;

            V_ERR_LOG_MSG := 'Json Response to Vlocity is failed for the given Ordernumber => '|| V_ORDERNUMBER;
            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
        END IF;
/* E2E Code Getting response from OV DP and Vlocity - Ends*/



/* Part 2*/

ELSE 
    BEGIN
        SELECT STATUS
        INTO V_ORDER_STATUS
        FROM
            JSON_ETL_PAYLOAD
        WHERE
            FILE_ID = V_FILE_ID ;

    v_log_message :=  'The Order status is => '|| NVL(V_ORDER_STATUS,'Null') ||' for the given Ordernumber => '|| V_ORDERNUMBER || '  in the JSON_ETL_PAYLOAD table';
    PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Order Status','PRC_NAYAN_MIGRATION_LOAD','Procedure - end','NEW', v_log_message, SYSDATE );

    EXCEPTION
    WHEN OTHERS THEN
    V_ERR_LOG_MSG := 'Order status record not exists for the given Ordernumber => '|| V_ORDERNUMBER;
    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

    END;

END IF;


V_END_TIME := dbms_utility.get_time;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Loading source tables and generating request order xml','PRC_NAYAN_MIGRATION_LOAD','Procedure - end','NEW', 'Processing Time = '||ROUND((V_END_TIME - V_START_TIME)/100,2)||' Seconds', SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_NAYAN_MIGRATION_LOAD', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_NAYAN_MIGRATION_LOAD;

PROCEDURE PRC_SRC_ORDER_DETAILS(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT             NUMBER := 0;
    V_DETAIL_CNT      NUMBER := 0;
	V_IS_CUS_ALPHANUM_VAL     VARCHAR2(1);   /*ADDED BY RUDHRA FOR ERROR HANDLING*/
	V_IS_CUS_ALPHANUM         VARCHAR2(4000);/*ADDED BY RUDHRA FOR ERROR HANDLING*/
	V_IS_CREATEDDATE          VARCHAR2(4000);/*ADDED BY RUDHRA FOR ERROR HANDLING*/
    V_error_code    NUMBER;


BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_SRC_ORDER_DETAILS','Received the Ordernumber from the vlocity Json source file','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );


                                                              SELECT
                                                              COUNT(1)
                                                              INTO V_CNT
                                                              FROM
                                                              NY_MIG_ORDER_DETAILS
                                                              WHERE
                                                               FILE_ID = V_FILE_ID 
                                                              AND STATUS = 'NEW';

                                                        IF V_CNT = 0 THEN

                                                          INSERT INTO NY_MIG_ORDER_DETAILS (
                                                               FILE_ID,
                                                               ORDER_ID,
                                                               ORDERNUMBER,
                                                               CONTACTID,
                                                               BILLINGACCOUNTNO,
                                                               CUSTOMERID,
                                                               BT_ORDER_STATUS__C,
                                                               CREATEDDATE,
                                                               EFFECTIVEDATE,
                                                               BT_ORDER_STATUS_PKL__C, -- Added by Siva
                                                               BRAND, -- Added by Siva
                                                               VLOCITY_CMT__REQUESTEDSTARTDATE__C,
                                                               VLOCITY_CMT__FULFILMENTSTATUS__C,
                                                               VLOCITY_CMT__ORIGINATINGCHANNEL__C,
                                                               VLOCITY_CMT__SUBMITTEDTOOMDATE__C,
                                                               ID,
                                                               NG_AGENTID__C,
                                                               NG_DEALERCODE__C,
                                                               NG_STORECODE__C,
                                                               STATUS
                                                              )
                                                                SELECT
                                                                FILE_ID,
                                                                ORDER_ID,
                                                                ORDERNUMBER,
                                                                CONTACTID,
                                                                BILLINGACCOUNTNO,                                            --Changes of Regrade
                                                                CUSTOMERID,
                                                                BT_ORDER_STATUS__C,
                                                                CREATEDDATE,
                                                                EFFECTIVEDATE,
                                                               BT_ORDER_STATUS_PKL__C, -- Added by Siva
                                                               BRAND, -- Added by Siva
                                                                VLOCITY_CMT__REQUESTEDSTARTDATE__C,
                                                                VLOCITY_CMT__FULFILMENTSTATUS__C,
                                                                VLOCITY_CMT__ORIGINATINGCHANNEL__C,
                                                                VLOCITY_CMT__SUBMITTEDTOOMDATE__C,
                                                                ID,
                                                                NG_AGENTID__C,
                                                                NG_DEALERCODE__C,
                                                                NG_STORECODE__C,
                                                                'NEW'                                                                
                                                                FROM
                                                                    VW_ORDER_DETAILS
                                                                WHERE
                                                                    FILE_ID = V_FILE_ID;

                                                                COMMIT;

                                                                  PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_SRC_ORDER_DETAILS','Order Details loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

                                                        ELSIF V_CNT > 0 THEN

                                                                  PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_SRC_ORDER_DETAILS','Order Detail already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

                                                                  V_ERR_LOG_MSG := 'The CustomerId from the vlocity Json source file is already exists';

                                                                  PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_ORDER_DETAILS', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);


                                                        END IF; --COUNT ENDS


/*ERROR HANDLING CHANGES DONE BY RUDHRA KUMAR NAGARAJAN FOR ORDER DETAILS ON 16/06/22 - STARTS*/


SELECT REPLACE(CUSTOMERID,' ','') AS CUSTOMERID, 
       REPLACE(CREATEDDATE,' ','') AS CREATEDDATE 
INTO   V_IS_CUS_ALPHANUM,
       V_IS_CREATEDDATE             
FROM   NY_MIG_ORDER_DETAILS 
WHERE  FILE_ID = V_FILE_ID ;                                                     


               IF V_IS_CREATEDDATE IS NULL THEN

                            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_SRC_ORDER_DETAILS','CREATEDDATE value is null in Json','NEW', 'Order Number => ' || NVL(V_ORDERNUMBER,'Null'), SYSDATE );

                            V_ERR_LOG_MSG := 'CREATEDDATE value is null in Json ';

                            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_ORDER_DETAILS', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

                            RETURN;

                ELSIF V_IS_CREATEDDATE IS NOT NULL THEN

                           PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_SRC_ORDER_DETAILS','Received the Ordernumber from the vlocity Json source file','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );


                END IF;  --V_IS_CREATEDDATE_VAL FULLY ENDS


                                    IF V_IS_CUS_ALPHANUM IS NULL THEN

                                            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_SRC_ORDER_DETAILS','CustomerID value is null in Json','NEW', 'Order Number => ' || NVL(V_ORDERNUMBER,'Null'), SYSDATE );

                                            V_ERR_LOG_MSG := 'CustomerID value is null in Json ';

                                            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_ORDER_DETAILS', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

                                            RETURN;

                                    ELSIF V_IS_CUS_ALPHANUM IS NOT NULL THEN   

                                            V_IS_CUS_ALPHANUM_VAL := CASE WHEN REGEXP_LIKE(V_IS_CUS_ALPHANUM, '[^A-Za-z0-9]') THEN 'N' ELSE 'Y' END;    

                                    IF V_IS_CUS_ALPHANUM_VAL = 'Y' THEN

                                            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_SRC_ORDER_DETAILS','Received the CustomerID from the vlocity Json source file','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

                                    ELSIF V_IS_CUS_ALPHANUM_VAL = 'N' THEN

                                            V_ERR_LOG_MSG := 'The CustomerId is having special Character';

                                            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_ORDER_DETAILS', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

                                             RETURN;                       	 

                                    END IF; --V_IS_CUS_ALPHANUM ENDS

                                    END IF; -- V_IS_CUS_ALPHANUM FULLY ENDS

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_SRC_ORDER_DETAILS','Procedure - end','NEW', v_log_message, SYSDATE );

/*ERROR HANDLING CHANGES DONE BY RUDHRA KUMAR NAGARAJAN FOR ORDER DETAILS ON 16/06/22* - ENDS*/

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_SRC_ORDER_DETAILS','Procedure - end','NEW', v_log_message, SYSDATE );


EXCEPTION

WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_ORDER_DETAILS', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_ORDER_DETAILS;

PROCEDURE PRC_SRC_ORDER_ATTRIBUTES(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_ATTRIBUTES from JSON Source file','PRC_SRC_ORDER_ATTRIBUTES','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_ORDER_ATTRIBUTES
        WHERE
             FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_ORDER_ATTRIBUTES (
                FILE_ID,
                ORD_ATTR_ID,
                ORDERNUMBER,
                BILLINGACCOUNTNO,
                PRODUCTCODE,
                PRODUCTID,
                ATTRIBUTENAME,
                ATTRIBUTEVALUE,
                STATUS,
				VLOCITY_CMT__ACTION__C,    -- Added by Bala for N 2 N Regrade 
				VLOCITY_CMT__SUBACTION__C  -- Added by Bala for N 2 N Regrade 

            )
                SELECT
                    FILE_ID,
                    ORD_ATTR_ID,
                    ORDERNUMBER,
                    BILLINGACCOUNTNO,
                    PRODUCTCODE,
                    PRODUCTID,
                    ATTRIBUTENAME,
                    ATTRIBUTEVALUE,
                    'NEW' as STATUS,
					VLOCITY_CMT__ACTION__C,
					VLOCITY_CMT__SUBACTION__C
                FROM
                    VW_ORDER_ATTRIBUTES
                WHERE
                    FILE_ID = V_FILE_ID
					and VLOCITY_CMT__ACTION__C <> 'Disconnect'; 

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_ATTRIBUTES from JSON Source file','PRC_SRC_ORDER_ATTRIBUTES','Order attributes loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_ATTRIBUTES from JSON Source file','PRC_SRC_ORDER_ATTRIBUTES','Order attributes already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_ATTRIBUTES from JSON Source file','PRC_SRC_ORDER_ATTRIBUTES','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_ORDER_ATTRIBUTES', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_ORDER_ATTRIBUTES;

PROCEDURE PRC_SRC_DISCOUNT_ATTRIBUTES(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the DISCOUNT_ATTRIBUTES from JSON Source file','PRC_SRC_DISCOUNT_ATTRIBUTES','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_DISCOUNT_ATTRIBUTES
        WHERE
             FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_DISCOUNT_ATTRIBUTES (
                FILE_ID,
                ORD_ATTR_ID,
                ORDERNUMBER,
                BILLINGACCOUNTNO,
                PRODUCTCODE,
                PRODUCTID,
                TIMEPLAN,
                NAME,
                DISCOUNTAMOUNT,
                STATUS,
				VLOCITY_CMT__ACTION__C,      -- Added for N 2 N REGRADE 
				VLOCITY_CMT__SUBACTION__C     -- Added for N 2 N REGRADE 
            )
                SELECT
                    FILE_ID,
                    ORD_ATTR_ID,
                    ORDERNUMBER,
                    BILLINGACCOUNTNO,
                    PRODUCTCODE,
                    PRODUCTID,
                    TIMEPLAN,
                    NAME,
                    DISCOUNTAMOUNT,
                    'NEW' as STATUS,
				VLOCITY_CMT__ACTION__C,
				VLOCITY_CMT__SUBACTION__C
                FROM
                    VW_DISCOUNT_ATTRIBUTES
                WHERE
                    FILE_ID = V_FILE_ID
					and VLOCITY_CMT__ACTION__C <> 'Disconnect';     -- Added for N 2 N REGRADE 

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the DISCOUNT_ATTRIBUTES from JSON Source file','PRC_SRC_DISCOUNT_ATTRIBUTES','Discount attributes loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the DISCOUNT_ATTRIBUTES from JSON Source file','PRC_SRC_DISCOUNT_ATTRIBUTES','Discount attributes already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the DISCOUNT_ATTRIBUTES from JSON Source file','PRC_SRC_DISCOUNT_ATTRIBUTES','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_DISCOUNT_ATTRIBUTES', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_DISCOUNT_ATTRIBUTES;

/* RAJU CODE CHAGES ADDED FOR NOWTV ADDON METADATA ON 11-08-22 START*/

PROCEDURE PRC_SRC_METADATA_ATTRIBUTES(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the METADATA_ATTRIBUTES from JSON Source file','PRC_SRC_METADATA_ATTRIBUTES','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_METADATA_ATTRIBUTES
        WHERE
             FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_METADATA_ATTRIBUTES (
                FILE_ID,                         
                ORD_ATTR_ID,                      
                ORDERNUMBER,                                                 
                BILLINGACCOUNTNO,                 
                SUPPLIERPRODUCTCODE,              
                SUPPLIERPRODUCTINSTANCEIDENTIFIER,
                CURRENTSUBSCRIPTIONCATEGORY,      
                CURRENTSUBSCRIPTIONSTARTDATE,     
                CURRENTSUBSCRIPTIONSTATE,         
				CURRENTSUBSCRIPTIONTYPE,             
				NEXTRENEWALPRICE,                 
                NEXTRENEWALORENDDATE,             
                PRODUCTNAME,                      
				PRODUCTID,                        
				BTBILLINGACCOUNTNO,               
				VLOCITY_CMT__ACTION__C,           
				VLOCITY_CMT__SUBACTION__C, 
                STATUS,
                PRODUCTCODE,
                FULFILLMENTINSTRUCTION

            )
                SELECT
                    FILE_ID,                         
                    ORD_ATTR_ID,                      
                    ORDERNUMBER,                                               
                    BILLINGACCOUNTNO,                 
                    SUPPLIERPRODUCTCODE,              
                    SUPPLIERPRODUCTINSTANCEIDENTIFIER,
                    CURRENTSUBSCRIPTIONCATEGORY,      
                    CURRENTSUBSCRIPTIONSTARTDATE,     
                    CURRENTSUBSCRIPTIONSTATE,         
				    CURRENTSUBSCRIPTIONTYPE,          
				    NEXTRENEWALPRICE,                 
                    NEXTRENEWALORENDDATE,             
                    PRODUCTNAME,                      
                    PRODUCTID,                        
                    BTBILLINGACCOUNTNO,               
                    VLOCITY_CMT__ACTION__C,           
                    VLOCITY_CMT__SUBACTION__C, 
					 'NEW' AS STATUS,
                     PRODUCTCODE,
                     FULFILLMENTINSTRUCTION

			   FROM
                    VW_METADATA_ATTRIBUTES
                WHERE
                    FILE_ID = V_FILE_ID;


            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the METADATA_ATTRIBUTES from JSON Source file','PRC_SRC_METADATA_ATTRIBUTES','Metadata attributes loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the METADATA_ATTRIBUTES from JSON Source file','PRC_SRC_METADATA_ATTRIBUTES','Metadata attributes already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the METADATA_ATTRIBUTES from JSON Source file','PRC_SRC_METADATA_ATTRIBUTES','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_METADATA_ATTRIBUTES', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_METADATA_ATTRIBUTES;

/* RAJU CODE CHANGES END FOR NOW TV ADDON ON 11-08-22 END*/

PROCEDURE PRC_SRC_ORDER_PRODUCT(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message VARCHAR2(4000);
    V_ERR_LOG_MSG VARCHAR2(1000);
    V_CNT NUMBER := 0;

BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PRODUCT from JSON Source file','PRC_SRC_ORDER_PRODUCT','Procedure - start','NEW', v_log_message, SYSDATE );


                            SELECT
                                COUNT(1)
                            INTO V_CNT
                            FROM
                                NY_MIG_ORDER_PRODUCT
                            WHERE
                                FILE_ID = V_FILE_ID
                                AND STATUS = 'NEW';

    IF V_CNT = 0 THEN

                        INSERT INTO NY_MIG_ORDER_PRODUCT (
                                FILE_ID                                     ,
                                ORD_PROD_ID                                 ,
                                ORDERNUMBER                                 ,
                                BILLINGACCOUNTNO                            ,
                                APPOINTMENT_REFERENCE__C                    ,
                                APPOINTMENTENDDATE                          ,
                                APPOINTMENTSTATUS                           ,
                                APPOINTMENTSTARTDATE                        ,
                                APPOINTMENTCREATEDDATE                      ,
                                APPOINTMENTPRODUCTTYPE                      ,
                                VLOCITY_CMT__ONETIMECHARGE__C               ,
                                VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C       ,
                                PRODUCTNAME                                 ,
                                VLOCITY_CMT__LINENUMBER__C                  ,
                                VLOCITY_CMT__FULFILMENTSTATUS__C            ,
                                VLOCITY_CMT__ISPONRREACHED__C               ,
                                VLOCITY_CMT__PROVISIONINGSTATUS__C          ,
                                PRODUCTCODE                                 ,
                                PRODUCTID                                   ,
                                NG_CUSTOMERREQUESTEDDATE__C                 ,
                                VLOCITY_CMT__RECURRINGCHARGE__C             ,
                                QUANTITY                                    ,
                                VLOCITY_CMT__RECURRINGTOTAL__C              ,
                                ORDERITEMNUMBER                             ,
                                VLOCITY_CMT__ACTION__C                      ,
                                COM_CALC_RECURRINGCHARGE__C                 ,
                                DELIVERY_INSTRUCTIONS__C                    ,
                                VLOCITY_CMT__TYPE__C                        ,  /* Changed by Paul on 20-May-2022 to align with select clause*/
                                STATUS                                     ,
                                APPOINTMENTDELIVERY_PHONE__C                ,/* CHANGED BY RUDHRA KUMAR ON 13-JUNE-2022 FOR NEW TAG FROM THE JSON */
                                APPOINTMENTENGINEER_INSTRUCTIONS__C         ,/* CHANGED BY RUDHRA KUMAR ON 13-JUNE-2022 FOR NEW TAG FROM THE JSON */
                                VLOCITY_CMT__ROOTITEMID__C                  ,/* CHANGED BY RUDHRA KUMAR ON 13-JUNE-2022 FOR NEW TAG FROM THE JSON */
                                VLOCITY_CMT__RECURRINGCALCULATEDPRICE__C    , /* CHANGED BY RUDHRA KUMAR ON 13-JUNE-2022 FOR NEW TAG FROM THE JSON */
                                VLOCITY_CMT__SUBACTION__C                   , /* Added Sub-action tag on 11-July-2022 */
                                CEASE_DATE                                  ,/* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                                FULFILLMENTINSTRUCTION                        /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */

                                            )
                                                SELECT
                                FILE_ID                                     ,
                                ORD_PROD_ID                                 ,
                                ORDERNUMBER                                 ,
                                BILLINGACCOUNTNO                                ,                       --Changes of Regrade
                                APPOINTMENT_REFERENCE__C                    ,
                                APPOINTMENTENDDATE                          ,
                                APPOINTMENTSTATUS                           ,
                                APPOINTMENTSTARTDATE                        ,
                                APPOINTMENTCREATEDDATE                      ,
                                APPOINTMENTPRODUCTTYPE                      ,
                                VLOCITY_CMT__ONETIMECHARGE__C               ,
                                VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C       ,
                                PRODUCTNAME                                 ,
                                VLOCITY_CMT__LINENUMBER__C                  ,
                                VLOCITY_CMT__FULFILMENTSTATUS__C            ,
                                VLOCITY_CMT__ISPONRREACHED__C               ,
                                VLOCITY_CMT__PROVISIONINGSTATUS__C          ,
                                PRODUCTCODE                                 ,
                                PRODUCTID                                   ,
                                NG_CUSTOMERREQUESTEDDATE__C                 ,
                                VLOCITY_CMT__RECURRINGCHARGE__C             ,
                                QUANTITY                                    ,
                                VLOCITY_CMT__RECURRINGTOTAL__C              ,
                                ORDERITEMNUMBER                             ,
                                VLOCITY_CMT__ACTION__C                      ,
                                COM_CALC_RECURRINGCHARGE__C                 ,
                                DELIVERY_INSTRUCTIONS__C                    ,
                                VLOCITY_CMT__TYPE__C                        , /* Renamed name from type to vlocity_cmt__Type__c on 30-June-2022*/
                                'NEW'                                       ,
                                APPOINTMENTDELIVERY_PHONE__C                ,/* CHANGED BY RUDHRA KUMAR ON 13-JUNE-2022 FOR NEW TAG FROM THE JSON */
                                APPOINTMENTENGINEER_INSTRUCTIONS__C         ,/* CHANGED BY RUDHRA KUMAR ON 13-JUNE-2022 FOR NEW TAG FROM THE JSON */
                                VLOCITY_CMT__ROOTITEMID__C                  ,/* CHANGED BY RUDHRA KUMAR ON 13-JUNE-2022 FOR NEW TAG FROM THE JSON */
                                VLOCITY_CMT__RECURRINGCALCULATEDPRICE__C    , /* CHANGED BY RUDHRA KUMAR ON 13-JUNE-2022 FOR NEW TAG FROM THE JSON */
                                VLOCITY_CMT__SUBACTION__C                   , /* Added Sub-action tag on 11-July-2022 */
                                CEASE_DATE                                  ,/* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                                FULFILLMENTINSTRUCTION                        /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */

                                                FROM
                                                    VW_ORDER_PRODUCT
                                                WHERE
                                                        FILE_ID = V_FILE_ID;



                                            COMMIT;

            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PRODUCT from JSON Source file','PRC_SRC_ORDER_PRODUCT','Order product records loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

    ELSIF V_CNT > 0 THEN

            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PRODUCT from JSON Source file','PRC_SRC_ORDER_PRODUCT','Order PRODUCT RECORDS already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

            V_ERR_LOG_MSG := 'The Order Product from the vlocity Json source file is already exists';

            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_ORDER_PRODUCT', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

    END IF;--COUNT ENDS                                                 		

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PRODUCT from JSON Source file','PRC_SRC_ORDER_PRODUCT','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_ORDER_PRODUCT', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_ORDER_PRODUCT;

PROCEDURE PRC_CHECK_APPOINTMENT_ATTR(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message VARCHAR2(4000);
    V_ERR_LOG_MSG VARCHAR2(1000);
    V_CNT NUMBER := 0;
    V_APP_CNT NUMBER := 0;                      /*ADDED BY RUDHRA FOR ERROR HANDLING*/
    V_APPOINTMENT_REFERENCE_C VARCHAR2(4000);   /*ADDED BY RUDHRA FOR ERROR HANDLING*/
    V_APPOINMENTREF_ALPHANUM VARCHAR2(1);       /*ADDED BY RUDHRA FOR ERROR HANDLING*/
    V_APPOINTMENT_PRODUCTTYPE VARCHAR2(4000);   /*ADDED BY RUDHRA FOR ERROR HANDLING*/
    V_APPOINMENTPRO_ALPHANUM VARCHAR2(1);       /*ADDED BY RUDHRA FOR ERROR HANDLING*/
    V_APPOINTMENTSTARTDATE VARCHAR2(4000);      /*ADDED BY RUDHRA FOR ERROR HANDLING*/
    V_APPOINTMENTENDDATE VARCHAR2(4000);        /*ADDED BY RUDHRA FOR ERROR HANDLING*/
    V_error_code NUMBER;

BEGIN
/*ERROR HANDLING CHANGES DONE BY RUDHRA KUMAR NAGARAJAN FOR ORDER PRODUCTS ON 16/06/22* - STARTS*/

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Validating the appointment section','PRC_CHECK_APPOINTMENT_ATTR','Procedure - start','NEW', v_log_message, SYSDATE );

     SELECT  COUNT (APPOINTMENT_REFERENCE__C||' '||APPOINTMENTSTARTDATE||' '||APPOINTMENTENDDATE||' '||APPOINTMENTPRODUCTTYPE)
     INTO V_APP_CNT
     FROM  NY_MIG_ORDER_PRODUCT
     WHERE FILE_ID = V_FILE_ID
     AND (PRODUCTNAME ='Managed Install'
     OR PRODUCTNAME ='Broadband Services'
     OR PRODUCTNAME ='Self Install')
     AND APPOINTMENTSTATUS= 'New';

IF V_APP_CNT > 0 THEN

                SELECT 
                      REPLACE(APPOINTMENT_REFERENCE__C,' ','') AS APPOINTMENT_REFERENCE__C,
                      REPLACE(APPOINTMENTSTARTDATE,' ','') AS APPOINTMENTSTARTDATE, 
                      REPLACE(APPOINTMENTENDDATE,' ','') AS APPOINTMENTENDDATE,
                      APPOINTMENTPRODUCTTYPE
                INTO  
                       V_APPOINTMENT_REFERENCE_C, V_APPOINTMENTSTARTDATE, V_APPOINTMENTENDDATE,V_APPOINTMENT_PRODUCTTYPE
                FROM 
                      NY_MIG_ORDER_PRODUCT
                WHERE
                      FILE_ID = V_FILE_ID
                AND 
                     (PRODUCTNAME ='Managed Install'
                      OR PRODUCTNAME ='Broadband Services'
                      OR PRODUCTNAME ='Self Install')
                AND 
                      APPOINTMENTSTATUS= 'New';

                        IF V_APPOINTMENT_REFERENCE_C IS NULL THEN

                                PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PRODUCT from JSON Source file','PRC_CHECK_APPOINTMENT_ATTR','APPOINTMENT_REFERENCE__C value is null in Json','NEW', 'Order Number => ' || NVL(V_ORDERNUMBER,'Null'), SYSDATE );

                                V_ERR_LOG_MSG := 'APPOINTMENT_REFERENCE__C value is null in Json ';

                                PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_CHECK_APPOINTMENT_ATTR', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

                                RETURN;

                        ELSIF V_APPOINTMENT_REFERENCE_C IS NOT NULL THEN

                                V_APPOINMENTREF_ALPHANUM := CASE WHEN REGEXP_LIKE(V_APPOINTMENT_REFERENCE_C, '[^A-Za-z0-9]') THEN 'N' ELSE 'Y' END;

                        IF V_APPOINMENTREF_ALPHANUM= 'Y' THEN

                                PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PRODUCT from JSON Source file','PRC_CHECK_APPOINTMENT_ATTR','Received the Ordernumber from the vlocity Json source file','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

                        ELSIF V_APPOINMENTREF_ALPHANUM = 'N' THEN

                                V_ERR_LOG_MSG := 'APPOINTMENT_REFERENCE__C is having Special Characters';

                                PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_CHECK_APPOINTMENT_ATTR', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

                                RETURN;

                        END IF;--V_APPOINMENTREF_ALPHANUM ENDS

                        END IF;--V_APPOINTMENT_REFERENCE_C FULLY ENDS

                                        IF V_APPOINTMENT_PRODUCTTYPE IS NULL THEN

                                                PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PRODUCT from JSON Source file','PRC_CHECK_APPOINTMENT_ATTR','APPOINTMENT_REFERENCE__C value is null in Json','NEW', 'Order Number => ' || NVL(V_ORDERNUMBER,'Null'), SYSDATE );

                                                V_ERR_LOG_MSG := 'APPOINTMENT_PRODUCTTYPE__C value is null in Json ';

                                                PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_CHECK_APPOINTMENT_ATTR', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);


                                        ELSIF V_APPOINTMENT_PRODUCTTYPE IS NOT NULL THEN

                                                V_APPOINMENTPRO_ALPHANUM := CASE WHEN REGEXP_LIKE(V_APPOINTMENT_PRODUCTTYPE, '[^A-Za-z0-9:space: \(\)-]') THEN 'N' ELSE 'Y' END;

                                        IF V_APPOINMENTPRO_ALPHANUM = 'Y' THEN

                                                PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PRODUCT from JSON Source file','PRC_CHECK_APPOINTMENT_ATTR','Received the Ordernumber from the vlocity Json source file','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

                                        ELSIF V_APPOINMENTPRO_ALPHANUM = 'N' THEN

                                                V_ERR_LOG_MSG := 'APPOINTMENT PRODUCTTYPE is having Special Characters';

                                                PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_CHECK_APPOINTMENT_ATTR', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

                                        END IF;--V_APPOINMENTPRO_ALPHANUM ENDS

                                        END IF;--V_APPOINTMENT_PRODUCTTYPE FULLY ENDS

                                                    IF V_APPOINTMENTSTARTDATE IS NULL THEN

                                                            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PRODUCT from JSON Source file','PRC_CHECK_APPOINTMENT_ATTR','APPOINTMENTSTARTDATE value is null in Json','NEW', 'Order Number => ' || NVL(V_ORDERNUMBER,'Null'), SYSDATE );

                                                            V_ERR_LOG_MSG := 'APPOINTMENTSTARTDATE value is null in Json ';

                                                            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_CHECK_APPOINTMENT_ATTR', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

                                                    ELSIF V_APPOINTMENTSTARTDATE IS NOT NULL THEN

                                                            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PRODUCT from JSON Source file','PRC_CHECK_APPOINTMENT_ATTR','Received the Ordernumber from the vlocity Json source file','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

                                                    END IF;--V_APPOINTMENTSTARTDATE ENDS

                                                                IF V_APPOINTMENTENDDATE IS NULL THEN

                                                                        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PRODUCT from JSON Source file','PRC_CHECK_APPOINTMENT_ATTR','APPOINTMENTSTARTDATE value is null in Json','NEW', 'Order Number => ' || NVL(V_ORDERNUMBER,'Null'), SYSDATE );

                                                                        V_ERR_LOG_MSG := 'APPOINTMENTENDDATE value is null in Json ';

                                                                        PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_CHECK_APPOINTMENT_ATTR', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);



                                                                ELSIF V_APPOINTMENTENDDATE IS NOT NULL THEN

                                                                        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PRODUCT from JSON Source file','PRC_CHECK_APPOINTMENT_ATTR','Received the Ordernumber from the vlocity Json source file','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

                                                                END IF;--V_APPOINTMENTENDDATE ENDS

ELSIF V_APP_CNT = 0 THEN

V_ERR_LOG_MSG := 'APPOINTMENT TAG IS NOT PROVIDED IN JSON ';
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_CHECK_APPOINTMENT_ATTR', SYSDATE, 'JSON_FILE_ERROR_LOG', V_ERR_LOG_MSG);

END IF;
/*ERROR HANDLING CHANGES DONE BY RUDHRA KUMAR NAGARAJAN FOR ORDER PRODUCTS ON 16/06/22* - Ends*/

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Validating the appointment section','PRC_CHECK_APPOINTMENT_ATTR','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_CHECK_APPOINTMENT_ATTR', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_CHECK_APPOINTMENT_ATTR;

PROCEDURE PRC_SRC_ORDER_PAYMENT(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS 

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PAYMENT from JSON Source file','PRC_SRC_ORDER_PAYMENT','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_ORDER_PAYMENT
        WHERE
             FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN

            INSERT INTO NY_MIG_ORDER_PAYMENT (   
FILE_ID,
ORD_PAY_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
PAYMENTEVENTTYPE,
PAYMENTEVENTSTATUS,
PaymentEventId,--added by akhil
CLIENTID,
PAYMENTAMOUNT,
ACTIVE,
ACCOUNT,
METHODTYPE,
STATUS,
PaymentGateway,--added by akhil
MerchantTxnRef,--added by akhil
CardType,--added by akhil
CardToken,--added by akhil
ExpiryMonth,--added by akhil
ExpiryYear        --added by akhil                            
            )
                SELECT
FILE_ID,
ORD_PAY_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
PAYMENTEVENTTYPE,
PAYMENTEVENTSTATUS,
PaymentEventId,--added by akhil
CLIENTID,
PAYMENTAMOUNT,
ACTIVE,
ACCOUNT,
METHODTYPE,
'NEW',
PaymentGateway,--added by akhil
MerchantTxnRef,
CardType,
CardToken,
ExpiryMonth,
ExpiryYear      --added by akhil
                FROM
                    VW_ORDER_PAYMENT
                WHERE
                    FILE_ID = V_FILE_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PAYMENT from JSON Source file','PRC_SRC_ORDER_PAYMENT','Order payment records loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PAYMENT from JSON Source file','PRC_SRC_ORDER_PAYMENT','Order payment records already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_PAYMENT from JSON Source file','PRC_SRC_ORDER_PAYMENT','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_ORDER_PAYMENT', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_ORDER_PAYMENT;


PROCEDURE PRC_SRC_CT_PT_ADDRESS(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the CONTACT POINT ADDRESS from JSON Source file','PRC_SRC_CT_PT_ADDRESS','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_CT_PT_ADDRESS
        WHERE
             FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_CT_PT_ADDRESS (
FILE_ID                              ,
CT_PT_ADDR_ID                        ,
ORDERNUMBER                          ,
BILLINGACCOUNTNO                     ,
ROBT_NAD_KEY__C                      ,
SYSTEMMODSTAMP                       ,
COUNTRY                              ,
NAD_KEY__C                           ,
PARENTID                             ,
CITY                                 ,
ISDEFAULT                            ,
LASTVIEWEDDATE                       ,
STREET                               ,
INST_ADDR_ROBTNADID                  ,
INST_ADDR_UPRN                       ,
INST_ADDR_STREETNR                   ,
INST_ADDR_STREETNAME                 ,
INST_ADDR_QUALIFIER                  ,
INST_ADDR_POSTCODE                   ,
INST_ADDR_POSTALORGANISATION         ,
INST_ADDR_POBOX                      ,
INST_ADDR_PARENTUPRN                 ,
INST_ADDR_LOCALITY                   ,
INST_ADDR_ID                         ,
INST_ADDR_GEOREFVAL                  ,
INST_ADDR_EXGRPCODE                  ,
INST_ADDR_DCODE                      ,
INST_ADDR_COUNTRY                    ,
INST_ADDR_CITY                       ,
INST_ADDR_TYPE                       ,
ISPRIMARY                            ,
POSTALCODE                           ,
LASTMODIFIEDDATE                     ,
ISDELETED                            ,
CREATEDDATE                          ,
CONTACTPOINTADDRESSID                ,
LASTREFERENCEDDATE                   ,
CREATEDBYID                          ,
LASTMODIFIEDBYID                     ,
NAME                                 ,
OWNERID                              ,
STATUS                               ,
STATE                                 /* CHANGED BY RUDHRA KUMAR ON 13-JUNE-2022 FOR NEW TAG FROM THE JSON */
)
                SELECT
FILE_ID                              ,
CT_PT_ADDR_ID                        ,
ORDERNUMBER                          ,
BILLINGACCOUNTNO                     ,
ROBT_NAD_KEY__C                      ,
SYSTEMMODSTAMP                       ,
COUNTRY                              ,
NAD_KEY__C                           ,
PARENTID                             ,
CITY                                 ,
ISDEFAULT                            ,
LASTVIEWEDDATE                       ,
STREET                               ,
INST_ADDR_ROBTNADID                  ,
INST_ADDR_UPRN                       ,
INST_ADDR_STREETNR                   ,
INST_ADDR_STREETNAME                 ,
INST_ADDR_QUALIFIER                  ,
INST_ADDR_POSTCODE                   ,
INST_ADDR_POSTALORGANISATION         ,
INST_ADDR_POBOX                      ,
INST_ADDR_PARENTUPRN                 ,
INST_ADDR_LOCALITY                   ,
INST_ADDR_ID                         ,
INST_ADDR_GEOREFVAL                  ,
INST_ADDR_EXGRPCODE                  ,
INST_ADDR_DCODE                      ,
INST_ADDR_COUNTRY                    ,
INST_ADDR_CITY                       ,
INST_ADDR_TYPE                       ,
ISPRIMARY                            ,
POSTALCODE                           ,
LASTMODIFIEDDATE                     ,
ISDELETED                            ,
CREATEDDATE                          ,
CONTACTPOINTADDRESSID                ,
LASTREFERENCEDDATE                   ,
CREATEDBYID                          ,
LASTMODIFIEDBYID                     ,
NAME                                 ,
OWNERID                              ,
   'NEW'                             ,
STATE                                 /* CHANGED BY RUDHRA KUMAR ON 13-JUNE-2022 FOR NEW TAG FROM THE JSON */
                FROM
                    VW_CONTACT_POINT_ADDRESS
                WHERE
                    FILE_ID = V_FILE_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the CONTACT POINT ADDRESS from JSON Source file','PRC_SRC_CT_PT_ADDRESS','Contact Point Address Details loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the CONTACT POINT ADDRESS from JSON Source file','PRC_SRC_CT_PT_ADDRESS','Contact Point Address Details already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the CONTACT POINT ADDRESS from JSON Source file','PRC_SRC_CT_PT_ADDRESS','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_CT_PT_ADDRESS', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_CT_PT_ADDRESS;

PROCEDURE PRC_SRC_CPA_GEOMETRY(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the CONTACT POINT ADDRESS Geometry from JSON Source file','PRC_SRC_CPA_GEOMETRY','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_CPA_GEOMETRY
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_CPA_GEOMETRY (
FILE_ID,
CPA_GEO_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
GEOMETRY_X,
GEOMETRY_Y,
STATUS                              
)
                SELECT
FILE_ID,
CPA_GEO_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
GEOMETRY_X,
GEOMETRY_Y,
   'NEW'
                FROM
                    VW_CPA_GEOMETRY
                WHERE
                    FILE_ID = V_FILE_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the CONTACT POINT ADDRESS Geometry from JSON Source file','PRC_SRC_CPA_GEOMETRY','Contact Point Address Details loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the CONTACT POINT ADDRESS Geometry from JSON Source file','PRC_SRC_CPA_GEOMETRY','Contact Point Address Details already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the CONTACT POINT ADDRESS Geometry from JSON Source file','PRC_SRC_CPA_GEOMETRY','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_CPA_GEOMETRY', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_CPA_GEOMETRY;


PROCEDURE PRC_SRC_CPA_GEO_SUBADDRESS(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the CONTACT POINT ADDRESS Geo Sub Address from JSON Source file','PRC_SRC_CPA_GEO_SUBADDRESS','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_CPA_GEO_SUBADDR
        WHERE
             FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_CPA_GEO_SUBADDR (
FILE_ID,
CPA_GEO_ADDR_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
GEO_SUBSTREET,
GEO_SUBLOCALITY,
GEO_SUBBUILDING,
GEO_BUILDINGNAME,
GEO_TYPE,
STATUS                              
)
                SELECT
FILE_ID,
CPA_GEO_ADDR_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
GEO_SUBSTREET,
GEO_SUBLOCALITY,
GEO_SUBBUILDING,
GEO_BUILDINGNAME,
GEO_TYPE,
   'NEW'
                FROM
                    VW_CPA_GEO_SUBADDR
                WHERE
                    FILE_ID = V_FILE_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the CONTACT POINT ADDRESS Geo Sub Address from JSON Source file','PRC_SRC_CPA_GEO_SUBADDRESS','Contact Point Address Details loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the CONTACT POINT ADDRESS Geo Sub Address from JSON Source file','PRC_SRC_CPA_GEO_SUBADDRESS','Contact Point Address Details already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the CONTACT POINT ADDRESS Geo Sub Address from JSON Source file','PRC_SRC_CPA_GEO_SUBADDRESS','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_CPA_GEO_SUBADDRESS', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_CPA_GEO_SUBADDRESS;


PROCEDURE PRC_SRC_TECH_ORDER_DETAIL(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TECHNICAL ORDER DETAIL from JSON Source file','PRC_SRC_TECH_ORDER_DETAIL','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_TECH_ORDER_DETAIL
        WHERE
             FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_TECH_ORDER_DETAIL 
(
FILE_ID,
TECHORDDET_ROWNUM,
ORDERNUMBER,
BILLINGACCOUNTNO,
TECHNICAL_ORDER_ID,
CORRELATIONID,
PROSPECT,
MOBILECOVERAGEREQUIRED,
CHANNEL,
JOURNEYTYPE,
SERVICETYPE,
CUSTOMERTYPE,
INST_ADDR_ID,
INST_ADDR_FTTPFIELDTYPE,
INST_ADDR_UPRN,
INST_ADDR_PARENTUPRN,
INST_ADDR_ADDRESSSOURCE,
INST_ADDR_EXCHANGEGROUPCODE,
INST_ADDR_DISTRICTCODE,
INST_ADDR_DIRECTORYNUMBER,
INST_ADDR_QUALIFIER,
INST_ADDR_STREETNR,
INST_ADDR_STREETNRLAST,
INST_ADDR_STREETNAME,
INST_ADDR_POSTALORGANISATION,
INST_ADDR_LOCALITY,
INST_ADDR_POSTCODE,
INST_ADDR_POBOX,
INST_ADDR_CITY,
INST_ADDR_COUNTRY,
EXCHANGE_CODE,
EXCHANGE_CSSEXCHANGETYPE,
EXCHANGE_GROUPCODE,
EXCHANGE_DISTRICT,
EXCHANGE_NAME,
LINE_SCENARIOSUMMARY,
LINE_SCENARIO,
LINE_SUBSCENARIO,
LINE_STOPPEDLINEQTY,
LINE_WORKINGLINEQTY,
LINE_VDSL,
LINE_GFAST,
LINE_ADSL,
LINE_BRIDGETAPINT_STATUS,
LINE_ACCESSLINETYPE,
LINE_ACCESSLINEID,
LINE_PRODUCTTYPE,
LINE_LINELOCATION,
LINE_INSTALLATIONTYPE,
LINE_PEND_CEASEORD_IND,
LINE_PEND_CEASE_COMP_DATE,
LINE_PARTIALDN,
LINE_CSSEXCHANGETYPE,
LINE_CSSEXCHANGECODE,
LINE_MDFSITEID,
LINE_NUMBERRETENTIONALLOWED,
LINE_LASTWORK_PAR_DIRECT_NUM,
LINE_STOPPEDDATE,
INST_DT_SERVICETYPE,
INST_DT_NUMBERRETENTIONALLOWED,
INST_DT_INSTALLATIONTYPE,
INST_DT_OWNERSHIPCONFIRMATION,
INST_DT_LINETYPE,
INST_DT_CSSEXCHANGETYPE,
INST_DT_CPSPRESENT,
INST_DT_CARELEVEL,
INST_DT_NARROWBANDSERVIND,
INST_DT_AUXLINEQUANTITY,
INST_DT_TEMPSTRUCTURE,
INST_DT_WHOLESALECALLSPRESENT,
INST_DT_DISTRICTCODE,
INST_DT_DIRECTORYENTRYLISTING,
INST_DT_DIRECTORYENTRYLINE,
INST_DT_CNFNAME,
DESIGNDISTRIBUTIONPOINT,
DISTRIBUTIONPOINTSPAREPAIRS,
DISTRIBUTIONPOINTTYPE,
BB_SERVICEPROVIDERNAME,
BB_FTTP_EXCHANGESTATE,
BB_FTTP_HIGHDOWNLOADSPEED,
BB_FTTP_ONTPRESENT,
BB_FTTP_FREEONTDATAPORTEXISTS,
BB_FTTP_L2SID,
BB_FTTP_PREMISETYPE,
BB_FTTP_SERVICENOTES,
BB_FTTP_TMAFLAG,
BB_FTTP_TMATYPE,
BB_FTTP_REC_MINIMUMLEADTIME,
BB_FTTP_FTTPINSTALLPROCESS,
BB_ISFTTPAPPOINTMENT1STAGE,
BB_FTTP_FTTPFIELDTYPE,
BB_FTTP_SCENARIOSUMMARY,
BB_FTTC_EXCHANGESTATE,
BB_FTTC_HIGHDOWNLOADSPEED,
BB_SOGEA_EXCHANGESTATE,
BB_SOGEA_HIGHDOWNLOADSPEED,
MOBILE_SS_2G,
MOBILE_SS_3G,
MOBILE_SS_4G800,
MOBILE_SS_4G1800,
MOBILE_SS_4G2600,
MOBILE_SS_4GPLUS,
MOBILE_SS_5G,
MOBILE_NC_POSTCODE,
MOBILE_NC_RAGSTATUS,
MOBILE_NC_USOELIGIBLE,
MOBILE_NC_SATELLITECOVERAGE,
MOBILE_NC_REPORTDATE,
MOBILE_NC_AVERAGEBAR,
MOBILE_NC_AVERAGESIGNAL,
MOBILE_4G_ISAVAILABLE,
MOBILE_4G_FAILTORETRIEVECOV,
MOBILE_4G_SIGNALSTRENGTH,
MOBILE_4G_MAXSPEED,
MOBILE_4GBB_ISAVAILABLE,
MOBILE_4GBB_SIGNALSTRENGTH,
MOBILE_4GBB_FAILTORETRIEVECOV,
MOBILE_4GBB_MAXSPEED,
MOBILE_5G_ISAVAILABLE,
MOBILE_5G_SIGNALSTRENGTH,
MOBILE_5G_FAILTORETRIEVECOV,
MOBILE_5G_MAXSPEED,
MOBILE_5GBB_ISAVAILABLE,
MOBILE_5GBB_SIGNALSTRENGTH,
MOBILE_5GBB_FAILTORETRIEVECOV,
MOBILE_5GBB_MAXSPEED,
DIRECTORYNUMBER_ID,
DIRECTORYNUMBER_NAME,
CATEGORY,
LIFECYCLESTATE,
PORTSTATE,
ROUTINGPREFIX,
BTRANGEMASTER,
SUBRINGFENCEID,
NUMBERPREFIX,
RINGFENCEID,
ONTSCENARIOSUMMARY,
STATUS)
                SELECT
FILE_ID,
TECHORDDET_ROWNUM,
ORDERNUMBER,
BILLINGACCOUNTNO,
TECHNICAL_ORDER_ID,
CORRELATIONID,
PROSPECT,
MOBILECOVERAGEREQUIRED,
CHANNEL,
JOURNEYTYPE,
SERVICETYPE,
CUSTOMERTYPE,
INST_ADDR_ID,
INST_ADDR_FTTPFIELDTYPE,
INST_ADDR_UPRN,
INST_ADDR_PARENTUPRN,
INST_ADDR_ADDRESSSOURCE,
INST_ADDR_EXCHANGEGROUPCODE,
INST_ADDR_DISTRICTCODE,
INST_ADDR_DIRECTORYNUMBER,
INST_ADDR_QUALIFIER,
INST_ADDR_STREETNR,
INST_ADDR_STREETNRLAST,
INST_ADDR_STREETNAME,
INST_ADDR_POSTALORGANISATION,
INST_ADDR_LOCALITY,
INST_ADDR_POSTCODE,
INST_ADDR_POBOX,
INST_ADDR_CITY,
INST_ADDR_COUNTRY,
EXCHANGE_CODE,
EXCHANGE_CSSEXCHANGETYPE,
EXCHANGE_GROUPCODE,
EXCHANGE_DISTRICT,
EXCHANGE_NAME,
LINE_SCENARIOSUMMARY,
LINE_SCENARIO,
LINE_SUBSCENARIO,
LINE_STOPPEDLINEQTY,
LINE_WORKINGLINEQTY,
LINE_VDSL,
LINE_GFAST,
LINE_ADSL,
LINE_BRIDGETAPINT_STATUS,
LINE_ACCESSLINETYPE,
LINE_ACCESSLINEID,
LINE_PRODUCTTYPE,
LINE_LINELOCATION,
LINE_INSTALLATIONTYPE,
LINE_PEND_CEASEORD_IND,
LINE_PEND_CEASE_COMP_DATE,
LINE_PARTIALDN,
LINE_CSSEXCHANGETYPE,
LINE_CSSEXCHANGECODE,
LINE_MDFSITEID,
LINE_NUMBERRETENTIONALLOWED,
LINE_LASTWORK_PAR_DIRECT_NUM,
LINE_STOPPEDDATE,
INST_DT_SERVICETYPE,
INST_DT_NUMBERRETENTIONALLOWED,
INST_DT_INSTALLATIONTYPE,
INST_DT_OWNERSHIPCONFIRMATION,
INST_DT_LINETYPE,
INST_DT_CSSEXCHANGETYPE,
INST_DT_CPSPRESENT,
INST_DT_CARELEVEL,
INST_DT_NARROWBANDSERVIND,
INST_DT_AUXLINEQUANTITY,
INST_DT_TEMPSTRUCTURE,
INST_DT_WHOLESALECALLSPRESENT,
INST_DT_DISTRICTCODE,
INST_DT_DIRECTORYENTRYLISTING,
INST_DT_DIRECTORYENTRYLINE,
INST_DT_CNFNAME,
DESIGNDISTRIBUTIONPOINT,
DISTRIBUTIONPOINTSPAREPAIRS,
DISTRIBUTIONPOINTTYPE,
BB_SERVICEPROVIDERNAME,
BB_FTTP_EXCHANGESTATE,
BB_FTTP_HIGHDOWNLOADSPEED,
BB_FTTP_ONTPRESENT,
BB_FTTP_FREEONTDATAPORTEXISTS,
BB_FTTP_L2SID,
BB_FTTP_PREMISETYPE,
BB_FTTP_SERVICENOTES,
BB_FTTP_TMAFLAG,
BB_FTTP_TMATYPE,
BB_FTTP_REC_MINIMUMLEADTIME,
BB_FTTP_FTTPINSTALLPROCESS,
BB_ISFTTPAPPOINTMENT1STAGE,
BB_FTTP_FTTPFIELDTYPE,
BB_FTTP_SCENARIOSUMMARY,
BB_FTTC_EXCHANGESTATE,
BB_FTTC_HIGHDOWNLOADSPEED,
BB_SOGEA_EXCHANGESTATE,
BB_SOGEA_HIGHDOWNLOADSPEED,
MOBILE_SS_2G,
MOBILE_SS_3G,
MOBILE_SS_4G800,
MOBILE_SS_4G1800,
MOBILE_SS_4G2600,
MOBILE_SS_4GPLUS,
MOBILE_SS_5G,
MOBILE_NC_POSTCODE,
MOBILE_NC_RAGSTATUS,
MOBILE_NC_USOELIGIBLE,
MOBILE_NC_SATELLITECOVERAGE,
MOBILE_NC_REPORTDATE,
MOBILE_NC_AVERAGEBAR,
MOBILE_NC_AVERAGESIGNAL,
MOBILE_4G_ISAVAILABLE,
MOBILE_4G_FAILTORETRIEVECOV,
MOBILE_4G_SIGNALSTRENGTH,
MOBILE_4G_MAXSPEED,
MOBILE_4GBB_ISAVAILABLE,
MOBILE_4GBB_SIGNALSTRENGTH,
MOBILE_4GBB_FAILTORETRIEVECOV,
MOBILE_4GBB_MAXSPEED,
MOBILE_5G_ISAVAILABLE,
MOBILE_5G_SIGNALSTRENGTH,
MOBILE_5G_FAILTORETRIEVECOV,
MOBILE_5G_MAXSPEED,
MOBILE_5GBB_ISAVAILABLE,
MOBILE_5GBB_SIGNALSTRENGTH,
MOBILE_5GBB_FAILTORETRIEVECOV,
MOBILE_5GBB_MAXSPEED,
DIRECTORYNUMBER_ID,
DIRECTORYNUMBER_NAME,
CATEGORY,
LIFECYCLESTATE,
PORTSTATE,
ROUTINGPREFIX,
BTRANGEMASTER,
SUBRINGFENCEID,
NUMBERPREFIX,
RINGFENCEID,
ONTSCENARIOSUMMARY,
'NEW' AS STATUS
                FROM
                    VW_TECH_ORDER_DETAIL
                WHERE
                    FILE_ID = V_FILE_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TECHNICAL ORDER DETAIL from JSON Source file','PRC_SRC_TECH_ORDER_DETAIL','Technical Order Detail records loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TECHNICAL ORDER DETAIL from JSON Source file','PRC_SRC_TECH_ORDER_DETAIL','Technical Order Detail records already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TECHNICAL ORDER DETAIL from JSON Source file','PRC_SRC_TECH_ORDER_DETAIL','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_TECH_ORDER_DETAIL', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_TECH_ORDER_DETAIL;

PROCEDURE PRC_SRC_TOD_GEOMETRY(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TECH ORDER DETAIL Geometry from JSON Source file','PRC_SRC_TOD_GEOMETRY','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_TOD_GEOMETRY
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_TOD_GEOMETRY (
FILE_ID,
TOD_GEO_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
GEOMETRY_X,
GEOMETRY_Y,
STATUS
)
                SELECT
FILE_ID,
TOD_GEO_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
GEOMETRY_X,
GEOMETRY_Y,
   'NEW'
                FROM
                    VW_TOD_GEOMETRY
                WHERE
                    FILE_ID = V_FILE_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TECH ORDER DETAIL Geometry from JSON Source file','PRC_SRC_TOD_GEOMETRY','TECH ORDER DETAIL Details loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TECH ORDER DETAIL Geometry from JSON Source file','PRC_SRC_TOD_GEOMETRY','TECH ORDER DETAIL Details already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TECH ORDER DETAIL Geometry from JSON Source file','PRC_SRC_TOD_GEOMETRY','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_TOD_GEOMETRY', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_TOD_GEOMETRY;

PROCEDURE PRC_SRC_TOD_GEO_SUBADDRESS(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TECH ORDER DETAIL Geo Sub Address from JSON Source file','PRC_SRC_TOD_GEO_SUBADDRESS','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_TOD_GEO_SUBADDR
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_TOD_GEO_SUBADDR (
FILE_ID,
TOD_GEO_ADDR_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
GEO_SUBSTREET,
GEO_SUBLOCALITY,
GEO_SUBBUILDING,
GEO_BUILDINGNAME,
GEO_TYPE,
STATUS
)
                SELECT
FILE_ID,
TOD_GEO_ADDR_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
GEO_SUBSTREET,
GEO_SUBLOCALITY,
GEO_SUBBUILDING,
GEO_BUILDINGNAME,
GEO_TYPE,
   'NEW'
                FROM
                    VW_TOD_GEO_SUBADDR
                WHERE
                    FILE_ID = V_FILE_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TECH ORDER DETAIL Geo Sub Address from JSON Source file','PRC_SRC_TOD_GEO_SUBADDRESS','TECH ORDER DETAIL  Details loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TECH ORDER DETAIL Geo Sub Address from JSON Source file','PRC_SRC_TOD_GEO_SUBADDRESS','TECH ORDER DETAIL  Details already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TECH ORDER DETAIL Geo Sub Address from JSON Source file','PRC_SRC_TOD_GEO_SUBADDRESS','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_TOD_GEO_SUBADDRESS', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_TOD_GEO_SUBADDRESS;


PROCEDURE PRC_SRC_TOD_SUPPLIEROFFERS(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD SUPPLIEROFFERS from JSON Source file','PRC_SRC_TOD_SUPPLIEROFFERS','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_SUPPLIEROFFERS
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_SUPPLIEROFFERS 
(
FILE_ID,
TECH_ORD_SUPP_ID,
TECHNICAL_ORDER_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
BB_SERVICE_TYPE,
SUPPLIEROFFERS_ID,
PRODUCTHIGHDOWNLOADSPEED,
PRODUCTLOWDOWNLOADSPEED,
PRODUCTHIGHUPLOADSPEED,
PRODUCTLOWUPLOADSPEED,
PRODUCTMAXIMUMDOWNLOADSPEED,
PRODUCTMAXIMUMUPLOADSPEED,
PRODUCTMINIMUMGUARANTEEDSPEED,
PRODUCTMINIMUMUPLOADSPEED,
STATUS
)
                SELECT
FILE_ID,
TECH_ORD_SUPP_ID,
TECHNICAL_ORDER_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
BB_SERVICE_TYPE,
SUPPLIEROFFERS_ID,
PRODUCTHIGHDOWNLOADSPEED,
PRODUCTLOWDOWNLOADSPEED,
PRODUCTHIGHUPLOADSPEED,
PRODUCTLOWUPLOADSPEED,
PRODUCTMAXIMUMDOWNLOADSPEED,
PRODUCTMAXIMUMUPLOADSPEED,
PRODUCTMINIMUMGUARANTEEDSPEED,
PRODUCTMINIMUMUPLOADSPEED,
'NEW' AS STATUS
                FROM
                    VW_TOD_FTTP_SUPPLIEROFFERS
                WHERE
                    FILE_ID = V_FILE_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD SUPPLIEROFFERS from JSON Source file','PRC_SRC_TOD_SUPPLIEROFFERS','TOD Supplieroffers records loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD SUPPLIEROFFERS from JSON Source file','PRC_SRC_TOD_SUPPLIEROFFERS','TOD Supplieroffers records already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD SUPPLIEROFFERS from JSON Source file','PRC_SRC_TOD_SUPPLIEROFFERS','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_TOD_SUPPLIEROFFERS', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_TOD_SUPPLIEROFFERS;

PROCEDURE PRC_SRC_TOD_ONTDETAILS(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD ONTDETAILS from JSON Source file','PRC_SRC_TOD_ONTDETAILS','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_ONTDETAILS
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_ONTDETAILS 
(
FILE_ID,
TECH_ORD_ONT_DET_ID,
TECHNICAL_ORDER_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
BB_SERVICE_TYPE,
DAMAGEDONT,
REFERENCE,
POSITION,
ROOM,
FLOOR,
SERIALNUMBER,
STATUS
)
                SELECT
FILE_ID,
TECH_ORD_ONT_DET_ID,
TECHNICAL_ORDER_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
BB_SERVICE_TYPE,
DAMAGEDONT,
REFERENCE,
POSITION,
ROOM,
FLOOR,
SERIALNUMBER,
'NEW' AS STATUS
                FROM
                    VW_TOD_FTTP_ONTDETAILS
                WHERE
                    FILE_ID = V_FILE_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD ONTDETAILS from JSON Source file','PRC_SRC_TOD_ONTDETAILS','TOD ONTDETAILS records loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD ONTDETAILS from JSON Source file','PRC_SRC_TOD_ONTDETAILS','TOD ONTDETAILS records already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD ONTDETAILS from JSON Source file','PRC_SRC_TOD_ONTDETAILS','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_TOD_ONTDETAILS', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_TOD_ONTDETAILS;


PROCEDURE PRC_SRC_TOD_ONTDETAILS_PORT(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD ONTDETAILS_PORT from JSON Source file','PRC_SRC_TOD_ONTDETAILS_PORT','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_ONTDETAILS_PORT
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_ONTDETAILS_PORT 
(
FILE_ID,
TECH_ORD_ONTDET_PORT_ID,
TECHNICAL_ORDER_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
BB_SERVICE_TYPE,
PORT_STATUS,
PORT_TYPE,
PORT_NUMBER,
STATUS
)
                SELECT
FILE_ID,
TECH_ORD_ONTDET_PORT_ID,
TECHNICAL_ORDER_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
BB_SERVICE_TYPE,
PORT_STATUS,
PORT_TYPE,
PORT_NUMBER,
'NEW' AS STATUS
                FROM
                    VW_TOD_FTTP_ONTDET_PORT
                WHERE
                    FILE_ID = V_FILE_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD ONTDETAILS_PORT from JSON Source file','PRC_SRC_TOD_ONTDETAILS_PORT','TOD ONTDETAILS_PORT records loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD ONTDETAILS_PORT from JSON Source file','PRC_SRC_TOD_ONTDETAILS_PORT','TOD ONTDETAILS_PORT records already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD ONTDETAILS_PORT from JSON Source file','PRC_SRC_TOD_ONTDETAILS_PORT','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_TOD_ONTDETAILS_PORT', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_TOD_ONTDETAILS_PORT;

PROCEDURE PRC_SRC_TOD_RELATED_PARTY(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD RELATED_PARTY from JSON Source file','PRC_SRC_TOD_RELATED_PARTY','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_RELATED_PARTY
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_RELATED_PARTY 
(
FILE_ID,
TECH_ORD_REL_PARTY_ID,
TECHNICAL_ORDER_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
RP_ROLE,
RP_NAME,
RP_TYPE,
STATUS
)
                SELECT
FILE_ID,
TECH_ORD_REL_PARTY_ID,
TECHNICAL_ORDER_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
RP_ROLE,
RP_NAME,
RP_TYPE,
'NEW' AS STATUS
                FROM
                    VW_TOD_RELATED_PARTY
                WHERE
                    FILE_ID = V_FILE_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD RELATED_PARTY from JSON Source file','PRC_SRC_TOD_RELATED_PARTY','TOD RELATED_PARTY records loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD RELATED_PARTY from JSON Source file','PRC_SRC_TOD_RELATED_PARTY','TOD RELATED_PARTY records already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD RELATED_PARTY from JSON Source file','PRC_SRC_TOD_RELATED_PARTY','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_TOD_RELATED_PARTY', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_TOD_RELATED_PARTY;


PROCEDURE PRC_SRC_VCODE_SCODE_MAP(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the Vlocity Scode mapping into Product Tree table','PRC_SRC_VCODE_SCODE_MAP','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_PRODUCT_TREE
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_PRODUCT_TREE (
MAPPING_TYPE,
PRODUCT_TYPE,
PRODUCT_ID,
S_CODE,
NAME,
LINK_TO_SERVICE_POINT,
CONTRACT_TERM,
PARENT_S_CODE,
PARENT,
RULES,
PRODUCT_FAMILY,
ATTRIBUTE_TYPE,
ATTRIBUTE_DISPLAY_NAME,
UNITPRICE,
DVCPE_DEVICE_QUANTITY,
DISCOUNTAMOUNT,
PRODUCTTERM,
PRODUCTTERMUNIT,
FILE_ID,
ORD_PROD_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
APPOINTMENT_REFERENCE__C,
APPOINTMENTENDDATE,
APPOINTMENTSTATUS,
APPOINTMENTSTARTDATE,
APPOINTMENTCREATEDDATE,
APPOINTMENTPRODUCTTYPE,
VLOCITY_CMT__ONETIMECHARGE__C,
VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
PRODUCTNAME,
VLOCITY_CMT__LINENUMBER__C,
VLOCITY_CMT__FULFILMENTSTATUS__C,
VLOCITY_CMT__ISPONRREACHED__C,
VLOCITY_CMT__PROVISIONINGSTATUS__C,
PRODUCTCODE,
PRODUCTID,
NG_CUSTOMERREQUESTEDDATE__C,
VLOCITY_CMT__RECURRINGCHARGE__C,
QUANTITY,
VLOCITY_CMT__RECURRINGTOTAL__C,
ORDERITEMNUMBER,
VLOCITY_CMT__ACTION__C,
COM_CALC_RECURRINGCHARGE__C,
DELIVERY_INSTRUCTIONS__C,
APPOINTMENTENGINEER_INSTRUCTIONS__C,
APPOINTMENTDELIVERY_PHONE__C,
VLOCITY_CMT__SUBACTION__C,                     /* Added Sub-action tag on 11-July-2022 */
STATUS,
OV_ACTION,                      /* Added by bala for N 2 N regrade */
FULFILLMENTINSTRUCTION, /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
CEASE_DATE              /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
)
                SELECT
    AM.MAPPING_TYPE,
    AM.PRODUCT_TYPE,
    AM.PRODUCT_ID,
    AM.S_CODE,
    AM.NAME,
    AM.LINK_TO_SERVICE_POINT,
    AM.CONTRACT_TERM,
    AM.PARENT_S_CODE,
    AM.PARENT,
    AM.RULES,
    AM.PRODUCT_FAMILY,
    AM.ATTRIBUTE_TYPE,
    AM.ATTRIBUTE_DISPLAY_NAME,
    (CASE WHEN INSTR ( ',' || AM.RULES || ','  , ',7,'  ) >0 THEN '[FM.recurringPrice]' 
            WHEN INSTR (',' || AM.RULES || ','  , ',8,'  ) >0 THEN '[FM.onetimePrice]'
            WHEN INSTR (',' || AM.RULES || ','  , ',12,'  ) >0 THEN '[FM.onetimePrice]'
            WHEN INSTR (',' || AM.RULES || ','  , ',13,'  ) >0 THEN '[FM.onetimePrice]' ELSE NULL END) AS unitPrice, /* SFI-31718 - BB Unit price issue - Added By Elen on 23/Jun/2022 */
    '' DVCPE_DEVICE_QUANTITY,
    '' DISCOUNTAMOUNT,
    '' PRODUCTTERM,
    '' PRODUCTTERMUNIT,
OP.FILE_ID,
OP.ORD_PROD_ID,
OP.ORDERNUMBER,
OP.BILLINGACCOUNTNO,
OP.APPOINTMENT_REFERENCE__C,
OP.APPOINTMENTENDDATE,
OP.APPOINTMENTSTATUS,
OP.APPOINTMENTSTARTDATE,
OP.APPOINTMENTCREATEDDATE,
OP.APPOINTMENTPRODUCTTYPE,
OP.VLOCITY_CMT__ONETIMECHARGE__C,
OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
OP.PRODUCTNAME,
OP.VLOCITY_CMT__LINENUMBER__C,
OP.VLOCITY_CMT__FULFILMENTSTATUS__C,
OP.VLOCITY_CMT__ISPONRREACHED__C,
OP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
OP.PRODUCTCODE,
OP.PRODUCTID,
OP.NG_CUSTOMERREQUESTEDDATE__C,
OP.VLOCITY_CMT__RECURRINGCHARGE__C,
OP.QUANTITY,
OP.VLOCITY_CMT__RECURRINGTOTAL__C,
OP.ORDERITEMNUMBER,
OP.VLOCITY_CMT__ACTION__C,
OP.COM_CALC_RECURRINGCHARGE__C,
OP.DELIVERY_INSTRUCTIONS__C,
OP.APPOINTMENTENGINEER_INSTRUCTIONS__C,
OP.APPOINTMENTDELIVERY_PHONE__C,
OP.VLOCITY_CMT__SUBACTION__C,                     /* Added Sub-action tag on 11-July-2022 */
 'NEW',
( CASE WHEN UPPER(OP.VLOCITY_CMT__ACTION__C) = 'EXISTING' THEN 'Update' 
        else 'Add' end )as OV_ACTION ,                     /* Added by bala for N 2 N regrade */
OP.FULFILLMENTINSTRUCTION,                  /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
OP.CEASE_DATE                               /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
FROM
    NY_MIG_ORDER_PRODUCT OP,
    DRIVER_ASSET_MAPPING AM
WHERE
    AM.PRODUCT_ID = OP.PRODUCTCODE
AND OP.FILE_ID = V_FILE_ID 
AND UPPER(AM.MAPPING_TYPE) <> 'REDUNDANT'
and (  VLOCITY_CMT__ACTION__C = 'Disconnect' 
	AND ( PRODUCTCODE IN ( 'N0001313','N0001036','N0001662','N0001664','N0001659','N0001651','N0001682' )) 
OR VLOCITY_CMT__ACTION__C <> 'Disconnect');    -- Added for N2N regrade scenario

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the Vlocity Scode mapping into Product Tree table','PRC_SRC_VCODE_SCODE_MAP','Product Tree records loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the Vlocity Scode mapping into Product Tree table','PRC_SRC_VCODE_SCODE_MAP','Product Tree records already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the Vlocity Scode mapping into Product Tree table','PRC_SRC_VCODE_SCODE_MAP','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_VCODE_SCODE_MAP', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_VCODE_SCODE_MAP;	

-- Regrade Changes. Added new parameter V_IS_REGRADE 
PROCEDURE PRC_SRC_PRODUCT_TREE_UPDATE(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER, V_IS_REGRADE IN VARCHAR2,V_REGRADE_TYPE IN VARCHAR2)AS
    v_rules NY_MIG_PRODUCT_TREE.rules%type;
    v_value             VARCHAR2(10);
    v_quantity          VARCHAR2(100);
    V_DVCPE_QUANTITY    VARCHAR2(100);
    v_log_message       VARCHAR2(4000);
    V_ERR_LOG_MSG       VARCHAR2(1000);
    V_CNT               NUMBER := 0;
    V_PRODUCTTREE_CNT   NUMBER := 0;
    V_PRODUCTTREE_BASE_CNT NUMBER:=0;
    V_ORDER_ATTR_CNT    NUMBER := 0;
    V_ORDER_ATTR_CNT_2    NUMBER := 0;
    V_ORDER_TMF_CNT     NUMBER := 0;
    V_ORDER_TMF_APP_CNT NUMBER := 0;
    V_ORDER_TMF_APP_CNT2 NUMBER := 0;
    V_APP_BRIT_VAL      VARCHAR2(10);
    V_ORDER_TMF_VAL     VARCHAR2(10);
    V_ORDER_TMF_APP_VAL VARCHAR2(10);
    V_ORDER_TMF_CNT2    NUMBER := 0;
    V_ORDER_PRD_CNT     NUMBER := 0;
    V_ORDER_PRD_APP_CNT NUMBER := 0;
    V_TMF_ORDER_PRD_CNT    NUMBER := 0;
    V_OFFER_CNT         NUMBER := 0;
    V_DISCOUNT_CNT      NUMBER := 0;
    V_PARENT_SCODE_CNT  NUMBER := 0;
    V_PHONENUM_ATTR_CNT NUMBER := 0;
    V_B_PROD_TYPE       VARCHAR2(50); /* Added by Paul on 13-Apr-22 */
    V_D_PROD_TYPE       VARCHAR2(50); /* Added by Paul on 13-Apr-22 */
    V_C_PROD_TYPE       VARCHAR2(50); /* Added by Paul on 13-Apr-22 */
    V_NAME              VARCHAR2(50); /* Added by Paul on 13-Apr-22 */
    V_PROD_CODE       	VARCHAR2(50); /* Added by Paul on 13-Apr-22 */
	V_OFFER_LIST       	VARCHAR2(1000); /* Added by Paul on 28-Apr-22 */
    V_CNF_PAR_VAL       VARCHAR2(1000);
    V_ATT_PAR_VAL       VARCHAR2(1000);
    ARE_BOTH_ADDON_BUNDLED EXCEPTION; /* Added by Paul on 15-May-22 */
    V_TYPE_ATTRIBUTE    VARCHAR2(50):=NULL;   /* Added by Paul on 15-May-22 */
    V_TYPE_ATTRIBUTE_A  VARCHAR2(50):=NULL;   /* Added by Paul on 15-May-22 */
    V_TYPE_ATTRIBUTE_B  VARCHAR2(50):=NULL;   /* Added by Paul on 15-May-22 */
    V_ADDON_CNT         NUMBER := 0;    /* Added by Paul on 15-May-22 */
    V_BUNDLED_CNT       NUMBER := 0;    /* Added by Paul on 15-May-22 */
    V_D_CNT             NUMBER := 0;    /* Added by Paul on 15-May-22 */
    V_PT_CNT            NUMBER := 0;   /* Added by Paul on 15-May-22 */

BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Product tree update','PRC_SRC_PRODUCT_TREE_UPDATE','Procedure - start','NEW', v_log_message, SYSDATE );

/* Start Code changes Added by Raju on 27-07-2022 for Restrict TV Products without tv promotions*/

SELECT COUNT(*) 
INTO V_ORDER_PRD_CNT
 FROM NY_MIG_PRODUCT_TREE MPT WHERE EXISTS (SELECT 1
    FROM NY_MIG_ORDER_PRODUCT MOP,DRIVER_ASSET_MAPPING DAM 
    WHERE MOP.FILE_ID = V_FILE_ID 
    AND MOP.PRODUCTCODE = DAM.PRODUCT_ID
    AND MPT.ORDERNUMBER = MOP.ORDERNUMBER
    AND MOP.PRODUCTCODE=MPT.PRODUCT_ID
    AND UPPER(MPT.PRODUCT_TYPE) = 'TVBASE'
    AND DAM.LINK_TYPE='TV');

IF V_ORDER_PRD_CNT = 0 THEN 


   DELETE FROM NY_MIG_PRODUCT_TREE MPT 
   WHERE EXISTS ( SELECT 1
   FROM NY_MIG_ORDER_PRODUCT MOP,DRIVER_ASSET_MAPPING DAM
   WHERE MOP.FILE_ID = V_FILE_ID 
   AND MOP.PRODUCTCODE = DAM.PRODUCT_ID
   AND MPT.ORDERNUMBER = MOP.ORDERNUMBER
   AND MOP.PRODUCTCODE=MPT.PRODUCT_ID
   AND DAM.LINK_TYPE='TV');

   COMMIT;

END IF;	

/* END  Code changes Added by Raju on 27-07-2022 for Restrict TV Products without tv promotions*/

FOR C IN (SELECT *
FROM NY_MIG_PRODUCT_TREE
WHERE FILE_ID = V_FILE_ID 
ORDER BY ORD_PROD_ID
)
LOOP

v_rules := C.RULES;

Begin

IF UPPER(C.PRODUCT_TYPE) = UPPER('BBBase') AND Regexp_count(v_rules,'6') >0 THEN

/*
    select val
    INTO v_value
    FROM (
    select regexp_substr(str, '[^,]+', 1, level) val
    from (select v_rules str from dual)
    connect by level <= length(str) - length(replace(str,','))+1
    )
    WHERE VAL = '6';
*/
	v_value := '6';

END IF;

Exception
WHEN NO_DATA_FOUND THEN
V_ERR_LOG_MSG := 'For the Product type => BBBase - Mapping Rule No 6 is not found';
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_PRODUCT_TREE_UPDATE', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
End;

/* Added by Paul on 29-Apr-2022 as part of SFI-24601 story for C1 delivery of S0331992 (BT TV 2 - PVR) :: Starts */
IF REGEXP_LIKE(UPPER(C.PRODUCT_ID),'N0001101|N0001102') THEN /*Updated by Paul on 27-May-2022 as per the recent update for rule 21 in the mapping Sheet*/
        DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND S_CODE = 'S0331992';
        COMMIT;        
END IF;    
/* Added by Paul on 29-Apr-2022 as part of SFI-24601 story for C1 delivery of S0331992 (BT TV 2 - PVR) :: Ends */

IF v_value = '6' AND UPPER(C.PRODUCT_TYPE) = UPPER('BBBase') THEN

    /* For product type BBBase check if we have (complete wifi) N0001094 product in the json if then consider only  product family BBCW, Bball, Blank as part of product mapping */
    SELECT COUNT(1) 
    INTO V_ORDER_PRD_CNT
    FROM NY_MIG_ORDER_PRODUCT
    WHERE FILE_ID = V_FILE_ID 
    AND PRODUCTCODE = 'N0001094';

	IF V_ORDER_PRD_CNT > 0 THEN

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Complete Wifi product mapping','PRC_SRC_PRODUCT_TREE_UPDATE','Removing BB product family','NEW', v_log_message, SYSDATE );

		DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND UPPER(PRODUCT_FAMILY) = UPPER('BB');
		COMMIT;

    ELSIF V_ORDER_PRD_CNT = 0 THEN

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Complete Wifi product mapping','PRC_SRC_PRODUCT_TREE_UPDATE','Removing BBCW product family','NEW', v_log_message, SYSDATE );

		DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND UPPER(PRODUCT_FAMILY) = UPPER('BBCW');
		COMMIT;

	END IF;

END IF;


/* Mapping type having child record but parent_s_code record is missing */
BEGIN

    IF UPPER(C.Mapping_type) = 'CHILD' THEN

        SELECT COUNT(1)
        INTO V_PARENT_SCODE_CNT
        FROM NY_MIG_PRODUCT_TREE
        WHERE FILE_ID = V_FILE_ID 
        AND S_CODE = C.PARENT_S_CODE;

        IF V_PARENT_SCODE_CNT = 0 THEN

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND PARENT_S_CODE = C.PARENT_S_CODE;
            COMMIT;

        END IF;

    END IF;

Exception
WHEN OTHERS THEN
V_ERR_LOG_MSG := 'Mapping type having child record but parent_s_code record is missing => PARENT_S_CODE - '|| C.PARENT_S_CODE;
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_PRODUCT_TREE_UPDATE', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
END;

/* Code changes ADDON and Bundled for TV on 16-Jun-22 by Paul Starts */
    IF UPPER(C.PRODUCT_TYPE) IN ('TVCPE','TVCPEADDON') THEN
        BEGIN
            SELECT COUNT(ATTRIBUTEVALUE) INTO V_CNT
            FROM NY_MIG_ORDER_ATTRIBUTES OA
			WHERE FILE_ID = V_FILE_ID 
            AND OA.PRODUCTCODE=C.PRODUCT_ID
			AND UPPER(ATTRIBUTENAME)='VEPC_ATTR_DT_PRODUCT_SPEC_TYPE'/* changes STANDARD to ADDON  by kanagavel*/
            AND UPPER(ATTRIBUTEVALUE)IN ('ADDON','BUNDLED')  /* changes STANDARD to ADDON by kanagavel*/
			AND EXISTS (SELECT 1 FROM NY_MIG_PRODUCT_TREE PT WHERE PT.ORDERNUMBER=OA.ORDERNUMBER
			--AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE)
            and PT.PRODUCTCODE = C.PRODUCT_ID);

    IF V_CNT>0 THEN /* Master Loop */
            SELECT COUNT(ATTRIBUTEVALUE) INTO V_CNT
            FROM NY_MIG_ORDER_ATTRIBUTES OA
			WHERE FILE_ID = V_FILE_ID 
            AND OA.PRODUCTCODE=C.PRODUCT_ID
			AND UPPER(ATTRIBUTENAME)='VEPC_ATTR_DT_PRODUCT_SPEC_TYPE'
            AND UPPER(ATTRIBUTEVALUE)= 'ADDON'
			AND EXISTS (SELECT 1 FROM NY_MIG_PRODUCT_TREE PT WHERE PT.ORDERNUMBER=OA.ORDERNUMBER
			--AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE)
            and PT.PRODUCTCODE = C.PRODUCT_ID);

            IF V_CNT>0 THEN           
			SELECT ATTRIBUTEVALUE,COUNT(1)
            INTO V_TYPE_ATTRIBUTE_A,V_ADDON_CNT
			FROM NY_MIG_ORDER_ATTRIBUTES OA
			WHERE FILE_ID = V_FILE_ID 
            AND OA.PRODUCTCODE=C.PRODUCT_ID
			AND UPPER(ATTRIBUTENAME)='VEPC_ATTR_DT_PRODUCT_SPEC_TYPE'
            AND UPPER(ATTRIBUTEVALUE) ='ADDON'
			AND EXISTS (SELECT 1 FROM NY_MIG_PRODUCT_TREE PT WHERE PT.ORDERNUMBER=OA.ORDERNUMBER
			--AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE)
            and PT.PRODUCTCODE = C.PRODUCT_ID)
            GROUP BY ATTRIBUTEVALUE;

            SELECT COUNT(PT.PRODUCTCODE) INTO V_PT_CNT
            FROM NY_MIG_PRODUCT_TREE PT
			WHERE PT.FILE_ID = V_FILE_ID 
            AND PT.PRODUCTCODE = C.PRODUCT_ID
			AND UPPER(PT.PRODUCT_TYPE||PT.S_CODE) IN (SELECT UPPER(BC.PRODUCT_TYPE||BC.S_CODE)
												   FROM NAYAN_ADDON_BUNDLED_CONFIG BC
													WHERE BC.PRODUCT_ID = C.PRODUCT_ID
													AND UPPER(BC.PRODUCT_FAMILY) = UPPER(V_TYPE_ATTRIBUTE_A));

        /*IF UPPER(V_TYPE_ATTRIBUTE_A) = 'ADDON' THEN*/
            V_D_CNT:=V_PT_CNT-V_ADDON_CNT;
            FOR X IN (SELECT ROWID FROM NY_MIG_PRODUCT_TREE PT
			WHERE PT.FILE_ID = V_FILE_ID 
            AND PT.PRODUCTCODE = C.PRODUCT_ID
			AND UPPER(PT.PRODUCT_TYPE||PT.S_CODE) IN (SELECT UPPER(BC.PRODUCT_TYPE||BC.S_CODE)
												   FROM NAYAN_ADDON_BUNDLED_CONFIG BC
													WHERE BC.PRODUCT_ID = C.PRODUCT_ID
													AND UPPER(BC.PRODUCT_FAMILY) = UPPER(V_TYPE_ATTRIBUTE_A)
												)
            AND ROWNUM<=V_D_CNT)
            LOOP
            DELETE FROM NY_MIG_PRODUCT_TREE PT
			WHERE PT.FILE_ID = V_FILE_ID 
            AND PT.PRODUCTCODE = C.PRODUCT_ID
            AND ROWID=X.ROWID;
            COMMIT;
            END LOOP;  

             /*Unit Price updated Starts*/
   IF Regexp_count(v_rules,'8')>0 then        
            UPDATE NY_MIG_PRODUCT_TREE PT
SET VLOCITY_CMT__ONETIMECHARGE__C =
(SELECT MAX(MOP.VLOCITY_CMT__ONETIMECHARGE__C)
    FROM NY_MIG_ORDER_ATTRIBUTES MOT,
	NY_MIG_ORDER_PRODUCT MOP
    WHERE MOT.FILE_ID = V_FILE_ID 
    AND UPPER(ATTRIBUTENAME)=  'VEPC_ATTR_DT_PRODUCT_SPEC_TYPE' 
    AND UPPER(ATTRIBUTEVALUE)= 'ADDON'
    AND MOP.PRODUCTCODE = MOT.PRODUCTCODE
	AND MOT.ORDERNUMBER=MOP.ORDERNUMBER
    AND MOP.PRODUCTCODE = C.PRODUCT_ID
    AND MOT.ORD_ATTR_ID = MOP.ORD_PROD_ID)
    WHERE PT.FILE_ID = V_FILE_ID 
            AND PT.PRODUCTCODE = C.PRODUCT_ID
			AND UPPER(PT.PRODUCT_TYPE||PT.S_CODE) IN (SELECT UPPER(BC.PRODUCT_TYPE||BC.S_CODE)
												   FROM NAYAN_ADDON_BUNDLED_CONFIG BC
													WHERE BC.PRODUCT_ID = C.PRODUCT_ID
													AND UPPER(BC.PRODUCT_FAMILY) = UPPER('ADDON'));
      COMMIT;  
      END IF;
   /*Unit Price updated Ends*/    

        ELSIF V_CNT=0 THEN
        DELETE FROM NY_MIG_PRODUCT_TREE PT
        WHERE PT.FILE_ID = V_FILE_ID 
        AND PT.PRODUCT_ID=C.PRODUCT_ID
        AND UPPER(PT.PRODUCT_TYPE||PT.S_CODE) IN (SELECT UPPER(BC.PRODUCT_TYPE||BC.S_CODE)
												   FROM NAYAN_ADDON_BUNDLED_CONFIG BC
													WHERE BC.PRODUCT_ID = C.PRODUCT_ID
													AND UPPER(BC.PRODUCT_FAMILY) ='ADDON');
        COMMIT; 
        END IF;

/* BUNDLED FIX */ 
            SELECT COUNT(ATTRIBUTEVALUE) INTO V_CNT
            FROM NY_MIG_ORDER_ATTRIBUTES OA
			WHERE FILE_ID = V_FILE_ID 
            AND OA.PRODUCTCODE=C.PRODUCT_ID
			AND UPPER(ATTRIBUTENAME)='VEPC_ATTR_DT_PRODUCT_SPEC_TYPE'
            AND UPPER(ATTRIBUTEVALUE)= 'BUNDLED'
			AND EXISTS (SELECT 1 FROM NY_MIG_PRODUCT_TREE PT WHERE PT.ORDERNUMBER=OA.ORDERNUMBER
			--AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE)
            and PT.PRODUCTCODE = C.PRODUCT_ID);

        IF V_CNT>0 THEN 

        SELECT ATTRIBUTEVALUE,COUNT(1)
            INTO V_TYPE_ATTRIBUTE_B,V_BUNDLED_CNT
			FROM NY_MIG_ORDER_ATTRIBUTES OA
			WHERE FILE_ID = V_FILE_ID 
            AND OA.PRODUCTCODE=C.PRODUCT_ID
			AND UPPER(ATTRIBUTENAME)='VEPC_ATTR_DT_PRODUCT_SPEC_TYPE'
            AND UPPER(ATTRIBUTEVALUE)='BUNDLED'
			AND EXISTS (SELECT 1 FROM NY_MIG_PRODUCT_TREE PT WHERE PT.ORDERNUMBER=OA.ORDERNUMBER
			--AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE)
            and PT.PRODUCTCODE = C.PRODUCT_ID)
            GROUP BY ATTRIBUTEVALUE;

        SELECT COUNT(PT.PRODUCTCODE) INTO V_PT_CNT
            FROM NY_MIG_PRODUCT_TREE PT
			WHERE PT.FILE_ID = V_FILE_ID 
            AND PT.PRODUCTCODE = C.PRODUCT_ID
			AND UPPER(PT.PRODUCT_TYPE||PT.S_CODE) IN (SELECT UPPER(BC.PRODUCT_TYPE||BC.S_CODE)
												   FROM NAYAN_ADDON_BUNDLED_CONFIG BC
													WHERE BC.PRODUCT_ID = C.PRODUCT_ID
													AND UPPER(BC.PRODUCT_FAMILY) = UPPER(V_TYPE_ATTRIBUTE_B));

            V_D_CNT:=0;
        /*IF UPPER(V_TYPE_ATTRIBUTE_B) = 'BUNDLED' THEN*/
            V_D_CNT:=V_PT_CNT-V_BUNDLED_CNT;
            FOR X IN (SELECT ROWID FROM NY_MIG_PRODUCT_TREE PT
			WHERE PT.FILE_ID = V_FILE_ID 
            AND PT.PRODUCTCODE = C.PRODUCT_ID
			AND UPPER(PT.PRODUCT_TYPE||PT.S_CODE) IN (SELECT UPPER(BC.PRODUCT_TYPE||BC.S_CODE)
												   FROM NAYAN_ADDON_BUNDLED_CONFIG BC
													WHERE BC.PRODUCT_ID = C.PRODUCT_ID
													AND UPPER(BC.PRODUCT_FAMILY) = UPPER(V_TYPE_ATTRIBUTE_B)
												)
            AND ROWNUM<=V_D_CNT)
            LOOP
            DELETE FROM NY_MIG_PRODUCT_TREE PT
			WHERE PT.FILE_ID = V_FILE_ID 
            AND PT.PRODUCTCODE = C.PRODUCT_ID
            AND ROWID=X.ROWID;
            COMMIT;
            END LOOP;
         /*Unit Price updated Starts*/
   IF Regexp_count(v_rules,'8')>0 then        
            UPDATE NY_MIG_PRODUCT_TREE PT
SET VLOCITY_CMT__ONETIMECHARGE__C =
(SELECT MAX(MOP.VLOCITY_CMT__ONETIMECHARGE__C)
    FROM NY_MIG_ORDER_ATTRIBUTES MOT,
	NY_MIG_ORDER_PRODUCT MOP
    WHERE MOT.FILE_ID = V_FILE_ID 
    AND UPPER(ATTRIBUTENAME)=  'VEPC_ATTR_DT_PRODUCT_SPEC_TYPE' 
    AND UPPER(ATTRIBUTEVALUE)= 'BUNDLED'
    AND MOP.PRODUCTCODE = MOT.PRODUCTCODE
	AND MOT.ORDERNUMBER=MOP.ORDERNUMBER
    AND MOP.PRODUCTCODE = C.PRODUCT_ID
    AND MOT.ORD_ATTR_ID = MOP.ORD_PROD_ID)
    WHERE PT.FILE_ID = V_FILE_ID 
            AND PT.PRODUCTCODE = C.PRODUCT_ID
			AND UPPER(PT.PRODUCT_TYPE||PT.S_CODE) IN (SELECT UPPER(BC.PRODUCT_TYPE||BC.S_CODE)
												   FROM NAYAN_ADDON_BUNDLED_CONFIG BC
													WHERE BC.PRODUCT_ID = C.PRODUCT_ID
													AND UPPER(BC.PRODUCT_FAMILY) = UPPER('BUNDLED'));
      COMMIT;  
      END IF;
   /*Unit Price updated Ends*/  
        ELSIF V_CNT=0 THEN
        DELETE FROM NY_MIG_PRODUCT_TREE PT
        WHERE PT.FILE_ID = V_FILE_ID 
        AND PT.PRODUCT_ID=C.PRODUCT_ID
        AND UPPER(PT.PRODUCT_TYPE||PT.S_CODE) IN (SELECT UPPER(BC.PRODUCT_TYPE||BC.S_CODE)
												   FROM NAYAN_ADDON_BUNDLED_CONFIG BC
													WHERE BC.PRODUCT_ID = C.PRODUCT_ID
													AND UPPER(BC.PRODUCT_FAMILY) ='BUNDLED');
        COMMIT;     
        END IF;
 /*Commented as Vinodh told let it deplicate if no proper value - Paul - 22-Jun-22 - Starts */
/*                SELECT COUNT(1)
            INTO V_CNT
			FROM NY_MIG_ORDER_ATTRIBUTES OA
			WHERE ORDERNUMBER =V_ORDERNUMBER
            AND OA.PRODUCTCODE=C.PRODUCT_ID
			AND UPPER(ATTRIBUTENAME)='VEPC_ATTR_DT_PRODUCT_TYPE'
            AND UPPER(ATTRIBUTEVALUE) IN ('ADDON','BUNDLED')
			AND EXISTS (SELECT 1 FROM NY_MIG_PRODUCT_TREE PT WHERE PT.ORDERNUMBER=OA.ORDERNUMBER
			--AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE)
            and PT.PRODUCTCODE = C.PRODUCT_ID);

        IF V_CNT=0 THEN
        DELETE FROM NY_MIG_PRODUCT_TREE PT
        WHERE PT.ORDERNUMBER=V_ORDERNUMBER
        AND PT.PRODUCT_ID=C.PRODUCT_ID;
        COMMIT;
        END IF;
        */ /*Commented as Vinodh told let it deplicate if no proper value - Paul - 22-Jun-22 - Ends */

       /* AND UPPER(PT.PRODUCT_TYPE||PT.S_CODE) IN (SELECT UPPER(BC.PRODUCT_TYPE||BC.S_CODE)
												   FROM NAYAN_ADDON_BUNDLED_CONFIG BC
													WHERE BC.PRODUCT_ID = C.PRODUCT_ID
													AND UPPER(BC.PRODUCT_FAMILY) <> UPPER(NVL(V_TYPE_ATTRIBUTE_A,V_TYPE_ATTRIBUTE_B))
												);*/
        /*ELSIF UPPER(NVL(V_TYPE_ATTRIBUTE_A,'N'))<>'ADDON' OR UPPER(NVL(V_TYPE_ATTRIBUTE_B,'N'))<>'BUNDLED'
        THEN
        DELETE FROM NY_MIG_PRODUCT_TREE PT
        WHERE PT.ORDERNUMBER=V_ORDERNUMBER
        AND PT.PRODUCT_ID=C.PRODUCT_ID
        AND UPPER(PT.PRODUCT_TYPE||PT.S_CODE) IN (SELECT UPPER(BC.PRODUCT_TYPE||BC.S_CODE)
												   FROM NAYAN_ADDON_BUNDLED_CONFIG BC
													WHERE BC.PRODUCT_ID = C.PRODUCT_ID
													AND UPPER(BC.PRODUCT_FAMILY) <> UPPER(NVL(V_TYPE_ATTRIBUTE_A,V_TYPE_ATTRIBUTE_B))
												);
        COMMIT;  */     
     END IF;    /* Master Loop */
        END;
    END IF; 
/* Code changes ADDON and Bundled for TV on 16-Jun-22 by Paul End */

/* Code changes ADDON and Bundled for TV on 10-Jun-22 by Paul and Ranganath Starts */
    IF UPPER(C.PRODUCT_TYPE) IN ('TVSPORTS','TVHD','TVUHD','TVHDBASE','TVUHDBASE','TVSPORTBASE') THEN
        BEGIN

            SELECT COUNT(1)
            INTO V_CNT
			FROM NY_MIG_ORDER_ATTRIBUTES OA
			WHERE FILE_ID = V_FILE_ID 
            AND OA.PRODUCTCODE=C.PRODUCT_ID
			AND UPPER(ATTRIBUTENAME)='VEPC_ATTR_DT_PRODUCT_SPEC_TYPE'
            AND UPPER(ATTRIBUTEVALUE) IN  ('ADDON','BUNDLED')
			AND EXISTS (SELECT 1 FROM NY_MIG_PRODUCT_TREE PT WHERE PT.ORDERNUMBER=OA.ORDERNUMBER
			--AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE)
            and PT.PRODUCTCODE = C.PRODUCT_ID);

       IF V_CNT>0 THEN       
            SELECT COUNT(1)
            INTO V_BUNDLED_CNT
			FROM NY_MIG_ORDER_ATTRIBUTES OA
			WHERE FILE_ID = V_FILE_ID 
            AND OA.PRODUCTCODE=C.PRODUCT_ID
			AND UPPER(ATTRIBUTENAME)='VEPC_ATTR_DT_PRODUCT_SPEC_TYPE'
            AND UPPER(ATTRIBUTEVALUE)='BUNDLED'
			AND EXISTS (SELECT 1 FROM NY_MIG_PRODUCT_TREE PT WHERE PT.ORDERNUMBER=OA.ORDERNUMBER
			--AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE)
            and PT.PRODUCTCODE = C.PRODUCT_ID);


            IF V_BUNDLED_CNT=0 THEN
            DELETE FROM NY_MIG_PRODUCT_TREE
            WHERE PRODUCTCODE = C.PRODUCT_ID 
                                    AND FILE_ID = V_FILE_ID  AND PRODUCT_TYPE||S_CODE IN 
                                        (SELECT PRODUCT_TYPE||S_CODE FROM NAYAN_ADDON_BUNDLED_CONFIG ABC
                                        WHERE ABC.PRODUCT_ID= C.PRODUCT_ID 
                                        AND UPPER(PRODUCT_FAMILY)=UPPER('BUNDLED'));
            COMMIT;

            ELSIF V_BUNDLED_CNT>0 THEN
                DELETE FROM NY_MIG_PRODUCT_TREE
                WHERE ROWID IN 
                    (SELECT ROWID FROM 
                            (SELECT PT.ROWID,PT.PRODUCTCODE,PT.ORDERNUMBER,PT.PRODUCT_TYPE,PT.PRODUCTNAME,PT.S_CODE
                                ,ROW_NUMBER() OVER( PARTITION BY PT.PRODUCT_TYPE||PT.S_CODE ORDER BY PRODUCT_TYPE DESC) AS LIM 
                                    FROM NY_MIG_PRODUCT_TREE PT
                                    WHERE PT.PRODUCTCODE = C.PRODUCT_ID 
                                    AND PT.FILE_ID = V_FILE_ID  AND PT.PRODUCT_TYPE||PT.S_CODE IN 
                                        (SELECT PRODUCT_TYPE||S_CODE FROM NAYAN_ADDON_BUNDLED_CONFIG ABC
                                        WHERE ABC.PRODUCT_ID= C.PRODUCT_ID 
                                        AND UPPER(PRODUCT_FAMILY)=UPPER('BUNDLED')))
                        WHERE LIM>V_BUNDLED_CNT)
                AND FILE_ID = V_FILE_ID 
                AND PRODUCTCODE = C.PRODUCT_ID;   
                COMMIT;
                 /*Unit Price updated Starts*/
   IF Regexp_count(v_rules,'7')>0 then        
            UPDATE NY_MIG_PRODUCT_TREE PT
SET COM_CALC_RECURRINGCHARGE__C =
(SELECT MAX(MOP.COM_CALC_RECURRINGCHARGE__C)
    FROM NY_MIG_ORDER_ATTRIBUTES MOT,
	NY_MIG_ORDER_PRODUCT MOP
    WHERE MOT.FILE_ID = V_FILE_ID 
    AND UPPER(ATTRIBUTENAME)=  'VEPC_ATTR_DT_PRODUCT_SPEC_TYPE' 
    AND UPPER(ATTRIBUTEVALUE)= 'BUNDLED'
    AND MOP.PRODUCTCODE = MOT.PRODUCTCODE
	AND MOT.ORDERNUMBER=MOP.ORDERNUMBER
    AND MOP.PRODUCTCODE = C.PRODUCT_ID
    AND MOT.ORD_ATTR_ID = MOP.ORD_PROD_ID)
    WHERE PT.FILE_ID = V_FILE_ID 
            AND PT.PRODUCTCODE = C.PRODUCT_ID
			AND UPPER(PT.PRODUCT_TYPE||PT.S_CODE) IN (SELECT UPPER(BC.PRODUCT_TYPE||BC.S_CODE)
												   FROM NAYAN_ADDON_BUNDLED_CONFIG BC
													WHERE BC.PRODUCT_ID = C.PRODUCT_ID
													AND UPPER(BC.PRODUCT_FAMILY) = UPPER('BUNDLED'));
      COMMIT;  
      END IF;
   /*Unit Price updated Ends*/  
            END IF;
  /*Addon Fix*/          
            SELECT COUNT(1)
            INTO V_ADDON_CNT
			FROM NY_MIG_ORDER_ATTRIBUTES OA
			WHERE FILE_ID = V_FILE_ID 
            AND OA.PRODUCTCODE=C.PRODUCT_ID
			AND UPPER(ATTRIBUTENAME)='VEPC_ATTR_DT_PRODUCT_SPEC_TYPE'
            AND UPPER(ATTRIBUTEVALUE)='ADDON'
			AND EXISTS (SELECT 1 FROM NY_MIG_PRODUCT_TREE PT WHERE PT.ORDERNUMBER=OA.ORDERNUMBER
			--AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE)
            and PT.PRODUCTCODE = C.PRODUCT_ID);

            IF V_ADDON_CNT=0 THEN
            DELETE FROM NY_MIG_PRODUCT_TREE
            WHERE PRODUCTCODE = C.PRODUCT_ID 
                                    AND FILE_ID = V_FILE_ID  AND PRODUCT_TYPE||S_CODE IN 
                                        (SELECT PRODUCT_TYPE||S_CODE FROM NAYAN_ADDON_BUNDLED_CONFIG ABC
                                        WHERE ABC.PRODUCT_ID= C.PRODUCT_ID 
                                        AND UPPER(PRODUCT_FAMILY)=UPPER('ADDON'));
            COMMIT;

            ELSIF V_ADDON_CNT>0 THEN
                DELETE FROM NY_MIG_PRODUCT_TREE
                WHERE ROWID IN 
                    (SELECT ROWID FROM 
                            (SELECT PT.ROWID,PT.PRODUCTCODE,PT.ORDERNUMBER,PT.PRODUCT_TYPE,PT.PRODUCTNAME,PT.S_CODE
                                ,ROW_NUMBER() OVER( PARTITION BY PT.PRODUCT_TYPE||PT.S_CODE ORDER BY PRODUCT_TYPE DESC) AS LIM 
                                    FROM NY_MIG_PRODUCT_TREE PT
                                    WHERE PT.PRODUCTCODE = C.PRODUCT_ID 
                                    AND PT.FILE_ID = V_FILE_ID  AND PT.PRODUCT_TYPE||PT.S_CODE IN 
                                        (SELECT PRODUCT_TYPE||S_CODE FROM NAYAN_ADDON_BUNDLED_CONFIG ABC
                                        WHERE ABC.PRODUCT_ID= C.PRODUCT_ID 
                                        AND UPPER(PRODUCT_FAMILY)=UPPER('ADDON')))
                        WHERE LIM>V_ADDON_CNT)
                AND FILE_ID = V_FILE_ID 
                AND PRODUCTCODE = C.PRODUCT_ID;   
                COMMIT;
/*Unit Price updated Starts*/
   IF Regexp_count(v_rules,'7')>0 then        
            UPDATE NY_MIG_PRODUCT_TREE PT
SET COM_CALC_RECURRINGCHARGE__C =
(SELECT MAX(MOP.COM_CALC_RECURRINGCHARGE__C)
    FROM NY_MIG_ORDER_ATTRIBUTES MOT,
	NY_MIG_ORDER_PRODUCT MOP
    WHERE MOT.FILE_ID = V_FILE_ID 
    AND UPPER(ATTRIBUTENAME)=  'VEPC_ATTR_DT_PRODUCT_SPEC_TYPE' 
    AND UPPER(ATTRIBUTEVALUE)= 'ADDON'
    AND MOP.PRODUCTCODE = MOT.PRODUCTCODE
	AND MOT.ORDERNUMBER=MOP.ORDERNUMBER
    AND MOP.PRODUCTCODE = C.PRODUCT_ID
    AND MOT.ORD_ATTR_ID = MOP.ORD_PROD_ID)
    WHERE PT.FILE_ID = V_FILE_ID 
            AND PT.PRODUCTCODE = C.PRODUCT_ID
			AND UPPER(PT.PRODUCT_TYPE||PT.S_CODE) IN (SELECT UPPER(BC.PRODUCT_TYPE||BC.S_CODE)
												   FROM NAYAN_ADDON_BUNDLED_CONFIG BC
													WHERE BC.PRODUCT_ID = C.PRODUCT_ID
													AND UPPER(BC.PRODUCT_FAMILY) = UPPER('ADDON'));
      COMMIT;  
      END IF;
   /*Unit Price updated Ends*/  
            END IF;

/*Commented as Vinodh told let it deplicate if no proper value - Paul - 22-Jun-22 - Starts */
  /*      SELECT COUNT(1)
            INTO V_CNT
			FROM NY_MIG_ORDER_ATTRIBUTES OA
			WHERE ORDERNUMBER =V_ORDERNUMBER
            AND OA.PRODUCTCODE=C.PRODUCT_ID
			AND UPPER(ATTRIBUTENAME)='VEPC_ATTR_DT_PRODUCT_TYPE'
            AND UPPER(ATTRIBUTEVALUE) IN ('ADDON','BUNDLED')
			AND EXISTS (SELECT 1 FROM NY_MIG_PRODUCT_TREE PT WHERE PT.ORDERNUMBER=OA.ORDERNUMBER
			--AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE)
            and PT.PRODUCTCODE = C.PRODUCT_ID);


        IF V_CNT=0 THEN
        DELETE FROM NY_MIG_PRODUCT_TREE PT
        WHERE PT.ORDERNUMBER=V_ORDERNUMBER
        AND PT.PRODUCT_ID=C.PRODUCT_ID;
        COMMIT;
        END IF;
        */
/*Commented as Vinodh told let it deplicate if no proper value - Paul - 22-Jun-22 - Ends */
    END IF;
        END;
    END IF; 
/* Code changes ADDON and Bundled for TV on 10-Jun-22 by Paul and Ranganath End */

/*-- Added by raju & kanagavel on 07-MAY-22 TVEMRBase Extra box logic - Starts*/

IF UPPER(C.PRODUCT_TYPE)  = 'TVEMRBASE'  THEN 

    SELECT COUNT (*)
    INTO V_ORDER_ATTR_CNT
    FROM NY_MIG_ORDER_ATTRIBUTES MOT, 
	NY_MIG_ORDER_PRODUCT MOP
    WHERE MOT.FILE_ID = V_FILE_ID 
    AND UPPER(ATTRIBUTENAME)=  'VEPC_ATTR_DT_PRODUCT_SPEC_TYPE'   --Needs to be change based on attribute name
    AND UPPER(ATTRIBUTEVALUE)= UPPER('Addon')
    AND MOP.PRODUCTCODE = MOT.PRODUCTCODE
	AND MOT.ORDERNUMBER=MOP.ORDERNUMBER
	AND UPPER(MOP.VLOCITY_CMT__TYPE__C)= UPPER('Equipment')
    AND MOT.ORD_ATTR_ID = MOP.ORD_PROD_ID
    AND MOT.ATTRIBUTEVALUE IS NOT NULL
	AND EXISTS (SELECT 1 
							FROM  NY_MIG_PRODUCT_TREE MPT 
							WHERE MPT.ORDERNUMBER= MOT.ORDERNUMBER 
                            AND MPT.PRODUCTCODE = MOT.PRODUCTCODE
							AND UPPER(PRODUCT_TYPE) =  UPPER('TVCPEAddOn'));


     IF V_ORDER_ATTR_CNT > 0 THEN   	 

     DELETE FROM NY_MIG_PRODUCT_TREE
     WHERE FILE_ID = V_FILE_ID 
     AND S_CODE  IN (SELECT PAR_VAL FROM NAYAN_CONFIG_PARAMS
     WHERE PAR_TYPE = 'Extra Box' 
     AND PAR_NAME < > V_ORDER_ATTR_CNT); 

 COMMIT;

 END IF;

 END IF;

   /*TVEMRBase extra box logic Added by Raju & Kanagavel ::ENDS*/

/*Pending Activation Marker product should be added against Amazon Offer.  part of C1 :: Added by kanagavel on 11-May-22 :: Sarts*/
IF UPPER(C.PRODUCT_TYPE) = UPPER('TVAmazonBase') THEN 

   --Check if Amazon offer product is created, if yes then create Activation Marker product and send the same in RO XML.
 SELECT COUNT(1)
    INTO  V_PRODUCTTREE_CNT
    FROM NY_MIG_PRODUCT_TREE OA
    WHERE FILE_ID = V_FILE_ID 
    AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.S_CODE = OA.S_CODE AND UPPER(PRODUCT_TYPE) = UPPER('TVAmazonOffer')); 

	IF  V_PRODUCTTREE_CNT = 0 THEN

	DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND UPPER(PRODUCT_TYPE) = UPPER('TVActMarker');
            COMMIT;
    END IF; 

END IF;	
/* Pending Activation Marker product should be added against Amazon Offer.  part of C1 :: Added by kanagavel on 11-May-22 :: Ends*/


/* Code changes ADDED for combining Contract product code for BB & TV 13-Apr-22 by Paul :: Starts */
/*If the contract is available json input then send the contract product in the xml*/
--IF v_value = '4' THEN 
--contract product tree validation Starts
IF REGEXP_LIKE(UPPER(C.PRODUCT_TYPE),upper('BBBase|TVBase|SportBase|BBSWBase|BBCHOPBASE')) THEN -- Update by Paul on 13-Apr-22

    SELECT COUNT(1) 
    INTO V_ORDER_ATTR_CNT
    FROM NY_MIG_ORDER_ATTRIBUTES
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(TRIM(ATTRIBUTENAME)) = 'VEPC_ATTR_RT_CONTRACT_DURATION'
	AND UPPER(PRODUCTCODE)=UPPER(C.PRODUCT_ID)
    AND ATTRIBUTEVALUE IS NOT NULL;
-- Added by Paul on 13-Apr-22 - Starts
  IF UPPER(C.PRODUCT_TYPE)  IN ('BBCHOPBASE','BBSWBASE' ) THEN

    SELECT PAR_TYPE, PAR_VAL
    INTO V_B_PROD_TYPE, V_C_PROD_TYPE
    FROM NAYAN_CONFIG_PARAMS
    WHERE UPPER(PAR_TYPE) = UPPER(C.PRODUCT_TYPE)
	AND UPPER(PAR_NAME) = 'CONTRACT';

 ELSE

    SELECT PAR_TYPE, PAR_VAL
    INTO V_B_PROD_TYPE, V_C_PROD_TYPE
    FROM NAYAN_CONFIG_PARAMS
    WHERE UPPER(PAR_TYPE) = UPPER(C.PRODUCT_TYPE);

  END IF;

     PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'PAR_VAL:',V_C_PROD_TYPE,'NEW', v_log_message, SYSDATE );

-- Added by Paul on 13-Apr-22 :: Ends    
    SELECT COUNT(1)
    INTO V_PRODUCTTREE_CNT
    FROM NY_MIG_PRODUCT_TREE
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(PRODUCT_TYPE) = UPPER(V_C_PROD_TYPE); -- Update by Paul on 13-Apr-22 

	IF V_ORDER_ATTR_CNT > 0 AND V_PRODUCTTREE_CNT = 0 THEN
		PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,V_C_PROD_TYPE||' product tree handling','PRC_SRC_PRODUCT_TREE_UPDATE','Loading the contract bundle into the product tree table','NEW', v_log_message, SYSDATE ); -- Update by Paul on 13-Apr-22 

            INSERT INTO NY_MIG_PRODUCT_TREE (
				MAPPING_TYPE,
				PRODUCT_TYPE,
				PRODUCT_ID,
				S_CODE,
				NAME,
				LINK_TO_SERVICE_POINT,
				CONTRACT_TERM,
				PARENT_S_CODE,
				PARENT,
				RULES,
				PRODUCT_FAMILY,
                ATTRIBUTE_TYPE,
                ATTRIBUTE_DISPLAY_NAME,
                UNITPRICE,
                DVCPE_DEVICE_QUANTITY,
                DISCOUNTAMOUNT,
                PRODUCTTERM,
                PRODUCTTERMUNIT,
				FILE_ID,
				ORD_PROD_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				APPOINTMENT_REFERENCE__C,
				APPOINTMENTENDDATE,
				APPOINTMENTSTATUS,
				APPOINTMENTSTARTDATE,
				APPOINTMENTCREATEDDATE,
				APPOINTMENTPRODUCTTYPE,
				VLOCITY_CMT__ONETIMECHARGE__C,
				VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
				PRODUCTNAME,
				VLOCITY_CMT__LINENUMBER__C,
				VLOCITY_CMT__FULFILMENTSTATUS__C,
				VLOCITY_CMT__ISPONRREACHED__C,
				VLOCITY_CMT__PROVISIONINGSTATUS__C,
				PRODUCTCODE,
				PRODUCTID,
				NG_CUSTOMERREQUESTEDDATE__C,
				VLOCITY_CMT__RECURRINGCHARGE__C,
				QUANTITY,
				VLOCITY_CMT__RECURRINGTOTAL__C,
				ORDERITEMNUMBER,
				VLOCITY_CMT__ACTION__C,
                COM_CALC_RECURRINGCHARGE__C,
                DELIVERY_INSTRUCTIONS__C, 
                APPOINTMENTENGINEER_INSTRUCTIONS__C,
                APPOINTMENTDELIVERY_PHONE__C,
 				STATUS,
                VLOCITY_CMT__SUBACTION__C,
				OV_ACTION ,                     /* Added by bala for N 2 N regrade */
                FULFILLMENTINSTRUCTION,         /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                CEASE_DATE                      /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
				)

SELECT 
				AM.MAPPING_TYPE,
				AM.PRODUCT_TYPE,
				OP.PRODUCTCODE AS PRODUCT_ID,
				AM.S_CODE,
				AM.NAME,
				AM.LINK_TO_SERVICE_POINT,
				AM.CONTRACT_TERM,
				AM.PARENT_S_CODE,
				AM.PARENT,
				AM.RULES,
				AM.PRODUCT_FAMILY,
                AM.ATTRIBUTE_TYPE,
                AM.ATTRIBUTE_DISPLAY_NAME,
                (CASE WHEN INSTR ( ',' || AM.RULES || ','  , ',7,'  ) >0 THEN '[FM.recurringPrice]' 
                        WHEN INSTR (',' || AM.RULES || ','  , ',8,'  ) >0 THEN '[FM.onetimePrice]'
                        WHEN INSTR (',' || AM.RULES || ','  , ',13,'  ) >0 THEN '[FM.onetimePrice]' ELSE NULL END) AS unitPrice, /* SFI-31718 - BB Unit price issue - Added By Elen on 23/Jun/2022 */
                '' DVCPE_DEVICE_QUANTITY,
                '' DISCOUNTAMOUNT,
                '' PRODUCTTERM,
                '' PRODUCTTERMUNIT,
			OP.FILE_ID,
			OP.ORD_PROD_ID,
			OP.ORDERNUMBER,
			OP.BILLINGACCOUNTNO,
			OP.APPOINTMENT_REFERENCE__C,
			OP.APPOINTMENTENDDATE,
			OP.APPOINTMENTSTATUS,
			OP.APPOINTMENTSTARTDATE,
			OP.APPOINTMENTCREATEDDATE,
			OP.APPOINTMENTPRODUCTTYPE,
			OP.VLOCITY_CMT__ONETIMECHARGE__C,
			OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
			OP.PRODUCTNAME,
			OP.VLOCITY_CMT__LINENUMBER__C,
			OP.VLOCITY_CMT__FULFILMENTSTATUS__C,
			OP.VLOCITY_CMT__ISPONRREACHED__C,
			OP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
			OP.PRODUCTCODE,
			OP.PRODUCTID,
			OP.NG_CUSTOMERREQUESTEDDATE__C,
			OP.VLOCITY_CMT__RECURRINGCHARGE__C,
			OP.QUANTITY,
			OP.VLOCITY_CMT__RECURRINGTOTAL__C,
			OP.ORDERITEMNUMBER,
			OP.VLOCITY_CMT__ACTION__C,
            OP.COM_CALC_RECURRINGCHARGE__C,
            OP.DELIVERY_INSTRUCTIONS__C,
            OP.APPOINTMENTENGINEER_INSTRUCTIONS__C,
            OP.APPOINTMENTDELIVERY_PHONE__C,
            OP.STATUS,
            OP.VLOCITY_CMT__SUBACTION__C,
			 'Add' as OV_ACTION ,                     /* Added by bala for N 2 N regrade */
             OP.FULFILLMENTINSTRUCTION,               /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
             OP.CEASE_DATE                            /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
			FROM
				NY_MIG_ORDER_PRODUCT OP,
				DRIVER_ASSET_MAPPING AM,
                (SELECT * FROM NY_MIG_ORDER_ATTRIBUTES OA WHERE OA.FILE_ID = V_FILE_ID  AND UPPER(OA.ATTRIBUTENAME) = UPPER('VEPC_ATTR_RT_CONTRACT_DURATION')
                 AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER(V_B_PROD_TYPE))) OA -- Updated by Paul on 13-Apr-22
			WHERE
			OP.ORDERNUMBER = OA.ORDERNUMBER
			AND OP.PRODUCTCODE = OA.PRODUCTCODE
			AND OP.FILE_ID = V_FILE_ID 
            AND UPPER(OP.VLOCITY_CMT__ACTION__C) <> UPPER('Disconnect')    -- Added for N2N regrade scenario
			AND UPPER(AM.MAPPING_TYPE) <> 'REDUNDANT'
			AND UPPER(AM.PRODUCT_ID) = UPPER('v_'||V_B_PROD_TYPE) -- Updated by Paul on 13-Apr-22
			AND UPPER(AM.PRODUCT_TYPE) IN  UPPER(V_C_PROD_TYPE); -- Updated by Paul on 13-Apr-22

			COMMIT;

	END IF;

END IF; -- contract product tree validation ends

/* Code changes ADDED for combining Contract product code for BB & TV 13-Apr-22 by Paul :: Ends */

/* Code changes ADDED for creating TVActivationCharge product for TVBase on 08-Apr-22 by Paul :: Starts */

IF UPPER(C.PRODUCT_TYPE) = UPPER('TVBase') THEN -- Update by Paul on 08-Apr-22  

    SELECT COUNT(1)
    INTO V_PRODUCTTREE_CNT
    FROM NY_MIG_PRODUCT_TREE
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(PRODUCT_TYPE) = UPPER('TVVision')	-- Update by Paul on 08-Apr-22 
	AND UPPER(PRODUCT_ID) = UPPER('v_TVBase');	-- Added by Paul on 08-Apr-22


	IF V_PRODUCTTREE_CNT = 0 THEN
		PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'TVActivationCharge product tree handling','PRC_SRC_PRODUCT_TREE_UPDATE','Loading the TVActivationCharge into the product tree table','NEW', v_log_message, SYSDATE );

            INSERT INTO NY_MIG_PRODUCT_TREE (
				MAPPING_TYPE,
				PRODUCT_TYPE,
				PRODUCT_ID,
				S_CODE,
				NAME,
				LINK_TO_SERVICE_POINT,
				CONTRACT_TERM,
				PARENT_S_CODE,
				PARENT,
				RULES,
				PRODUCT_FAMILY,
                ATTRIBUTE_TYPE,
                ATTRIBUTE_DISPLAY_NAME,
                UNITPRICE,
                DVCPE_DEVICE_QUANTITY,
                DISCOUNTAMOUNT,
                PRODUCTTERM,
                PRODUCTTERMUNIT,
				FILE_ID,
				ORD_PROD_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				APPOINTMENT_REFERENCE__C,
				APPOINTMENTENDDATE,
				APPOINTMENTSTATUS,
				APPOINTMENTSTARTDATE,
				APPOINTMENTCREATEDDATE,
				APPOINTMENTPRODUCTTYPE,
				VLOCITY_CMT__ONETIMECHARGE__C,
				VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
				PRODUCTNAME,
				VLOCITY_CMT__LINENUMBER__C,
				VLOCITY_CMT__FULFILMENTSTATUS__C,
				VLOCITY_CMT__ISPONRREACHED__C,
				VLOCITY_CMT__PROVISIONINGSTATUS__C,
				PRODUCTCODE,
				PRODUCTID,
				NG_CUSTOMERREQUESTEDDATE__C,
				VLOCITY_CMT__RECURRINGCHARGE__C,
				QUANTITY,
				VLOCITY_CMT__RECURRINGTOTAL__C,
				ORDERITEMNUMBER,
				VLOCITY_CMT__ACTION__C,
                COM_CALC_RECURRINGCHARGE__C,
                DELIVERY_INSTRUCTIONS__C,
                APPOINTMENTENGINEER_INSTRUCTIONS__C,
                APPOINTMENTDELIVERY_PHONE__C,
				STATUS ,
                VLOCITY_CMT__SUBACTION__C,
                OV_ACTION ,                     /* Added by bala for N 2 N regrade */	
                FULFILLMENTINSTRUCTION,         /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                CEASE_DATE                      /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
				)

SELECT 
				AM.MAPPING_TYPE,
				AM.PRODUCT_TYPE,
				OP.PRODUCTCODE AS PRODUCT_ID,
				AM.S_CODE,
				AM.NAME,
				AM.LINK_TO_SERVICE_POINT,
				AM.CONTRACT_TERM,
				AM.PARENT_S_CODE,
				AM.PARENT,
				AM.RULES,
				AM.PRODUCT_FAMILY,
                AM.ATTRIBUTE_TYPE,
                AM.ATTRIBUTE_DISPLAY_NAME,
                (CASE WHEN INSTR ( ',' || AM.RULES || ','  , ',7,'  ) >0 THEN '[FM.recurringPrice]' 
                        WHEN INSTR (',' || AM.RULES || ','  , ',8,'  ) >0 THEN '[FM.onetimePrice]'
                        WHEN INSTR (',' || AM.RULES || ','  , ',12,'  ) >0 THEN '[FM.onetimePrice]' ELSE NULL END) AS unitPrice, /* SFI-31718 - BB Unit price issue - Added By Elen on 23/Jun/2022 */
                '' DVCPE_DEVICE_QUANTITY,
                '' DISCOUNTAMOUNT,
                '' PRODUCTTERM,
                '' PRODUCTTERMUNIT,
			OP.FILE_ID,
			OP.ORD_PROD_ID,
			OP.ORDERNUMBER,
			OP.BILLINGACCOUNTNO,
			OP.APPOINTMENT_REFERENCE__C,
			OP.APPOINTMENTENDDATE,
			OP.APPOINTMENTSTATUS,
			OP.APPOINTMENTSTARTDATE,
			OP.APPOINTMENTCREATEDDATE,
			OP.APPOINTMENTPRODUCTTYPE,
			OP.VLOCITY_CMT__ONETIMECHARGE__C,
			OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
			OP.PRODUCTNAME,
			OP.VLOCITY_CMT__LINENUMBER__C,
			OP.VLOCITY_CMT__FULFILMENTSTATUS__C,
			OP.VLOCITY_CMT__ISPONRREACHED__C,
			OP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
			OP.PRODUCTCODE,
			OP.PRODUCTID,
			OP.NG_CUSTOMERREQUESTEDDATE__C,
			OP.VLOCITY_CMT__RECURRINGCHARGE__C,
			OP.QUANTITY,
			OP.VLOCITY_CMT__RECURRINGTOTAL__C,
			OP.ORDERITEMNUMBER,
			OP.VLOCITY_CMT__ACTION__C,
            OP.COM_CALC_RECURRINGCHARGE__C,
            OP.DELIVERY_INSTRUCTIONS__C,
            OP.APPOINTMENTENGINEER_INSTRUCTIONS__C,
            OP.APPOINTMENTDELIVERY_PHONE__C,
			OP.STATUS,
            OP.VLOCITY_CMT__SUBACTION__C,
			 'Add' as OV_ACTION ,                     /* Added by bala for N 2 N regrade */
             OP.FULFILLMENTINSTRUCTION,               /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
             OP.CEASE_DATE                            /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
			FROM
				NY_MIG_ORDER_PRODUCT OP,
				DRIVER_ASSET_MAPPING AM
			WHERE
			OP.FILE_ID = V_FILE_ID 
            AND UPPER(OP.VLOCITY_CMT__ACTION__C) <> UPPER('Disconnect')    -- Added for N2N regrade scenario
			AND UPPER(AM.MAPPING_TYPE) <> 'REDUNDANT'
			AND UPPER(AM.PRODUCT_ID) = UPPER('v_TVBase')  -- Update by Paul on 08-Apr-22 
			AND UPPER(AM.PRODUCT_TYPE) IN  UPPER('TVVision')  -- Update by Paul on 08-Apr-22
			AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OP.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('TVBase'));

			COMMIT;

	END IF;


END IF; -- TVActivationCharge product tree validation ends

/* Code changes ADDED for creating TVActivationCharge product for TVBase on 08-Apr-22 by Paul :: Ends */

/* Code changes ADDED for combined Discount product code for BB & TV 13-Apr-22 by Paul :: Starts */
/* Added by Paul on 28-Apr-2022 as part of C1 delivery :: Starts */
    SELECT COUNT(1) 
    INTO V_DISCOUNT_CNT
    FROM NAYAN_CONFIG_PARAMS
    WHERE PAR_TYPE ='DISCOUNT_OFFER_LIST';

    IF V_DISCOUNT_CNT>0 THEN
    SELECT PAR_VAL
    INTO V_OFFER_LIST
    FROM NAYAN_CONFIG_PARAMS
    WHERE PAR_TYPE ='DISCOUNT_OFFER_LIST';
    END IF;
/* Added by Paul on 28-Apr-2022 as part of C1 delivery :: Ends */

/*IF REGEXP_LIKE(UPPER(C.PRODUCT_TYPE),'BBOffer|TVOffer') THEN -- Update by Paul on 13-Apr-22 as part of C1 */
IF REGEXP_LIKE(UPPER(C.PRODUCT_TYPE),V_OFFER_LIST) THEN /* Update by Paul on 28-Apr-22 as part of C1 */

/* Added by Paul on 23-Jun-2022 as part of Addon and Bundled - Starts */
 /*   SELECT COUNT(1) INTO V_CNT
            FROM NY_MIG_PRODUCT_TREE PT
			WHERE PT.ORDERNUMBER=V_ORDERNUMBER
            AND PT.PRODUCTCODE = C.PRODUCT_ID
            AND PT.PRODUCTCODE IN ('N0001024','N0001076','N0001131')
            AND UPPER(PT.PRODUCT_TYPE) IN ('TVHDDISCOUNT','TVSPORTDISCOUNT','TVUHDDISCOUNT');
    IF V_CNT=0 THEN    */        
/* Added by Paul on 23-Jun-2022 as part of Addon and Bundled - Ends */
  IF UPPER(C.PRODUCT_TYPE)  IN ('BBCHOPBASE','BBSWBASE','BBCSOFFER' ) THEN

    SELECT PAR_TYPE, PAR_VAL
    INTO V_B_PROD_TYPE, V_D_PROD_TYPE
    FROM NAYAN_CONFIG_PARAMS
    WHERE UPPER(PAR_TYPE) = UPPER(C.PRODUCT_TYPE)
	AND UPPER(PAR_NAME) = 'DISCOUNT';

 ELSE

    SELECT PAR_NAME, PAR_VAL
    INTO V_B_PROD_TYPE, V_D_PROD_TYPE
    FROM NAYAN_CONFIG_PARAMS
    WHERE UPPER(PAR_TYPE) = UPPER(C.PRODUCT_TYPE);

  END IF;

       PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'V_D_PROD_TYPE:',V_D_PROD_TYPE,'NEW', v_log_message, SYSDATE );


    SELECT COUNT(1) 
    INTO V_DISCOUNT_CNT
    FROM NY_MIG_DISCOUNT_ATTRIBUTES DA
    WHERE DA.FILE_ID = V_FILE_ID 
    AND DA.TIMEPLAN IS NOT NULL
    AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = DA.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER(V_B_PROD_TYPE)); -- Updated by Paul on 13-Apr-22



    IF V_DISCOUNT_CNT = 0 AND  UPPER(C.PRODUCT_TYPE) NOT IN ('BBCHOPBASE','BBSWBASE','BBCSOFFER' )  THEN

            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,C.PRODUCT_TYPE||' and '||V_D_PROD_TYPE||' product tree handling',
            'PRC_SRC_PRODUCT_TREE_UPDATE','Simplyfy universal discount and offer not exists in Json input','NEW', v_log_message, SYSDATE ); -- Updated by Paul on 13-Apr-22 

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE); -- Updated by Paul on 13-Apr-22
            COMMIT;

    END IF; 

    SELECT COUNT(1)
    INTO V_PRODUCTTREE_CNT
    FROM NY_MIG_PRODUCT_TREE
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(PRODUCT_TYPE) = UPPER(V_D_PROD_TYPE); -- Updated by Paul on 13-Apr-22


       PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'disc:V_DISCOUNT_CNT:',V_DISCOUNT_CNT,'NEW', v_log_message, SYSDATE );

       PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'disc :V_PRODUCTTREE_CNT:',V_PRODUCTTREE_CNT,'NEW', v_log_message, SYSDATE );


	IF V_DISCOUNT_CNT > 0 AND V_PRODUCTTREE_CNT = 0 THEN

		PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,V_D_PROD_TYPE||' product tree handling','PRC_SRC_PRODUCT_TREE_UPDATE','Loading the Simplyfy universal discount into the product tree table','NEW', v_log_message, SYSDATE ); -- Updated by Paul on 13-Apr-22

            INSERT INTO NY_MIG_PRODUCT_TREE (
				MAPPING_TYPE,
				PRODUCT_TYPE,
				PRODUCT_ID,
				S_CODE,
				NAME,
				LINK_TO_SERVICE_POINT,
				CONTRACT_TERM,
				PARENT_S_CODE,
				PARENT,
				RULES,
				PRODUCT_FAMILY,
                ATTRIBUTE_TYPE,
                ATTRIBUTE_DISPLAY_NAME,
                UNITPRICE,
                DVCPE_DEVICE_QUANTITY,
                DISCOUNTAMOUNT,
                PRODUCTTERM,
                PRODUCTTERMUNIT,
				FILE_ID,
				ORD_PROD_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				APPOINTMENT_REFERENCE__C,
				APPOINTMENTENDDATE,
				APPOINTMENTSTATUS,
				APPOINTMENTSTARTDATE,
				APPOINTMENTCREATEDDATE,
				APPOINTMENTPRODUCTTYPE,
				VLOCITY_CMT__ONETIMECHARGE__C,
				VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
				PRODUCTNAME,
				VLOCITY_CMT__LINENUMBER__C,
				VLOCITY_CMT__FULFILMENTSTATUS__C,
				VLOCITY_CMT__ISPONRREACHED__C,
				VLOCITY_CMT__PROVISIONINGSTATUS__C,
				PRODUCTCODE,
				PRODUCTID,
				NG_CUSTOMERREQUESTEDDATE__C,
				VLOCITY_CMT__RECURRINGCHARGE__C,
				QUANTITY,
				VLOCITY_CMT__RECURRINGTOTAL__C,
				ORDERITEMNUMBER,
				VLOCITY_CMT__ACTION__C,
                COM_CALC_RECURRINGCHARGE__C,
                DELIVERY_INSTRUCTIONS__C,
                APPOINTMENTENGINEER_INSTRUCTIONS__C,
                APPOINTMENTDELIVERY_PHONE__C,
				STATUS ,
                VLOCITY_CMT__SUBACTION__C,
                OV_ACTION,                      /* Added by bala for N 2 N regrade */	
                FULFILLMENTINSTRUCTION,         /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                CEASE_DATE                      /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
				)

SELECT 
				AM.MAPPING_TYPE,
				AM.PRODUCT_TYPE,
				OP.PRODUCTCODE AS PRODUCT_ID,
				NVL(DM.S_CODE,DM1.S_CODE) AS S_CODE,
				NVL(DM.S_CODE_NAME,DM1.S_CODE_NAME) AS NAME,
				AM.LINK_TO_SERVICE_POINT,
				AM.CONTRACT_TERM,
				NVL(DM.S_CODE,DM1.S_CODE)||NVL(DM.DISCOUNT,DM1.DISCOUNT) AS PARENT_S_CODE, /* Added by Paul to handle duplicate scode on 18-May-2022 */
				NVL(DM.S_CODE_NAME,DM1.S_CODE_NAME) AS PARENT,
				AM.RULES,
				AM.PRODUCT_FAMILY,
                AM.ATTRIBUTE_TYPE,
                AM.ATTRIBUTE_DISPLAY_NAME,
                NVL(DM.UNITPRICE,DM1.UNITPRICE) AS unitPrice,
                '' DVCPE_DEVICE_QUANTITY,
                DS.DISCOUNTAMOUNT AS DISCOUNTAMOUNT,
                (CASE WHEN NVL(DM.PRODUCTTERM,DM1.PRODUCTTERM) = '[PM.discountTerm]' THEN (REGEXP_REPLACE(DS.TIMEPLAN,'TP-|M')) ELSE NULL END) AS PRODUCTTERM,
                NVL(DM.PRODUCTTERMUNIT,DM1.PRODUCTTERMUNIT) AS PRODUCTTERMUNIT,
			OP.FILE_ID,
			OP.ORD_PROD_ID,
			OP.ORDERNUMBER,
			OP.BILLINGACCOUNTNO,
			OP.APPOINTMENT_REFERENCE__C,
			OP.APPOINTMENTENDDATE,
			OP.APPOINTMENTSTATUS,
			OP.APPOINTMENTSTARTDATE,
			OP.APPOINTMENTCREATEDDATE,
			OP.APPOINTMENTPRODUCTTYPE,
			OP.VLOCITY_CMT__ONETIMECHARGE__C,
			OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
			DS.NAME AS PRODUCTNAME,
			OP.VLOCITY_CMT__LINENUMBER__C,
			OP.VLOCITY_CMT__FULFILMENTSTATUS__C,
			OP.VLOCITY_CMT__ISPONRREACHED__C,
			OP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
			OP.PRODUCTCODE,
			OP.PRODUCTID,
			OP.NG_CUSTOMERREQUESTEDDATE__C,
			OP.VLOCITY_CMT__RECURRINGCHARGE__C,
			OP.QUANTITY,
			OP.VLOCITY_CMT__RECURRINGTOTAL__C,
			OP.ORDERITEMNUMBER,
			OP.VLOCITY_CMT__ACTION__C,
            OP.COM_CALC_RECURRINGCHARGE__C,
            OP.DELIVERY_INSTRUCTIONS__C,
            OP.APPOINTMENTENGINEER_INSTRUCTIONS__C,
            OP.APPOINTMENTDELIVERY_PHONE__C,
			OP.STATUS,
            OP.VLOCITY_CMT__SUBACTION__C,
			 'Add'  as OV_ACTION,                      /* Added by bala for N 2 N regrade */
             OP.FULFILLMENTINSTRUCTION,                /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
             OP.CEASE_DATE                             /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
			FROM
				NY_MIG_ORDER_PRODUCT OP,
				DRIVER_ASSET_MAPPING AM,
                (select * from DRIVER_DISCOUNT_MAPPING where UPPER(DISCOUNT) = UPPER('v_'||V_D_PROD_TYPE)) DM, -- Updated by Paul on 13-Apr-22 
                (SELECT * FROM DRIVER_DISCOUNT_MAPPING WHERE TIMEPLAN IS NULL AND UPPER(DISCOUNT) = UPPER('v_'||V_D_PROD_TYPE)) DM1, -- Updated by Paul on 13-Apr-22
                (SELECT * FROM NY_MIG_DISCOUNT_ATTRIBUTES DA WHERE DA.FILE_ID = V_FILE_ID  
                 AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = DA.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER(V_B_PROD_TYPE))) DS -- Updated by Paul on 13-Apr-22
			WHERE
			OP.ORDERNUMBER = DS.ORDERNUMBER 
            AND OP.PRODUCTCODE = DS.PRODUCTCODE
            AND OP.FILE_ID = V_FILE_ID 
            AND UPPER(OP.VLOCITY_CMT__ACTION__C) <> UPPER('Disconnect')    -- Added for N2N regrade scenario
			AND DS.TIMEPLAN = DM.TIMEPLAN(+)
			AND UPPER(AM.MAPPING_TYPE) <> 'REDUNDANT'
			AND UPPER(AM.PRODUCT_ID) = UPPER('v_'||V_B_PROD_TYPE) -- Updated by Paul on 13-Apr-22
			AND UPPER(AM.PRODUCT_TYPE) =  UPPER(V_D_PROD_TYPE); -- Updated by Paul on 13-Apr-22
			COMMIT;
 /* Deleting Mulple discount for Addon and Bundled by Paul on 24-Jun-22 - Starts*/           
DELETE FROM NY_MIG_PRODUCT_TREE
 WHERE ROWID NOT IN (SELECT max(rowid) 
            FROM NY_MIG_PRODUCT_TREE PT
			WHERE PT.FILE_ID = V_FILE_ID 
            AND PT.PRODUCTCODE = C.PRODUCT_ID
            AND PT.PRODUCTCODE IN ('N0001024','N0001076','N0001131')
            AND UPPER(PT.PRODUCT_TYPE) IN ('TVHDDISCOUNT','TVSPORTDISCOUNT','TVUHDDISCOUNT'))
            AND FILE_ID = V_FILE_ID 
            AND PRODUCTCODE = C.PRODUCT_ID
            AND PRODUCTCODE IN ('N0001024','N0001076','N0001131')
            AND UPPER(PRODUCT_TYPE) IN ('TVHDDISCOUNT','TVSPORTDISCOUNT','TVUHDDISCOUNT'); 
            commit;
/* Deleting Mulple discount for Addon and Bundled by Paul on 24-Jun-22 - Starts*/       

/*Unit Price updated Starts*/
   IF Regexp_count(C.RULES,'5')>0 then
            UPDATE NY_MIG_PRODUCT_TREE PT
SET COM_CALC_RECURRINGCHARGE__C =
(SELECT MAX(MOP.COM_CALC_RECURRINGCHARGE__C)
    FROM NY_MIG_ORDER_ATTRIBUTES MOT,
	NY_MIG_ORDER_PRODUCT MOP
    WHERE MOT.FILE_ID = V_FILE_ID 
    AND UPPER(ATTRIBUTENAME)=  'VEPC_ATTR_DT_PRODUCT_SPEC_TYPE' 
    AND UPPER(ATTRIBUTEVALUE)= 'ADDON'
    AND MOP.PRODUCTCODE = MOT.PRODUCTCODE
	AND MOT.ORDERNUMBER=MOP.ORDERNUMBER
    AND MOP.PRODUCTCODE = C.PRODUCT_ID
    AND MOT.ORD_ATTR_ID = MOP.ORD_PROD_ID)
    WHERE PT.FILE_ID = V_FILE_ID 
            AND PT.PRODUCTCODE = C.PRODUCT_ID
            AND PRODUCTCODE IN ('N0001024','N0001076','N0001131')
            AND UPPER(PRODUCT_TYPE) IN ('TVHDDISCOUNT','TVSPORTDISCOUNT','TVUHDDISCOUNT');
      COMMIT;  
      END IF;
   /*Unit Price updated Ends*/  

    END IF; 
	/*END IF; */ /* Added by Paul on 23-Jun-2022 as part of Addon and Bundled - Ends */
    END IF; -- Offer and Discount product tree handling ends

/* Code changes ADDED for combined Discount product code for BB & TV 13-Apr-22 by Paul :: Ends */

/*Code changes added by raju palle on 22-08-22 for GCP Reverse mapping start*/

IF UPPER(C.PRODUCT_TYPE)  IN  ('TVASIANBASE','TVBOXBASE','TVCOMBIBASE','TVCOMBIHDBASE','TVHDBASE','TVSPORTBASE','TVUHDBASE','BBCSOFFER') THEN

 IF  UPPER(C.PRODUCT_TYPE) = 'BBCSOFFER' THEN

    SELECT PAR_TYPE,PAR_NAME,PAR_VAL
    INTO V_B_PROD_TYPE,V_C_PROD_TYPE,V_D_PROD_TYPE
    FROM NAYAN_CONFIG_PARAMS
   WHERE UPPER(PAR_TYPE) = UPPER(C.PRODUCT_TYPE)
   AND UPPER(PAR_NAME) = 'BBCS';

ELSE

   SELECT PAR_TYPE,PAR_NAME,PAR_VAL
    INTO V_B_PROD_TYPE,V_C_PROD_TYPE,V_D_PROD_TYPE
    FROM NAYAN_CONFIG_PARAMS
    WHERE UPPER(PAR_TYPE) = UPPER(C.PRODUCT_TYPE);

END IF;


	SELECT COUNT(1)
    INTO V_PRODUCTTREE_CNT
    FROM NY_MIG_PRODUCT_TREE
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(PRODUCT_TYPE) = UPPER(V_C_PROD_TYPE);

    SELECT COUNT(1)
    INTO V_PRODUCTTREE_BASE_CNT
    FROM NY_MIG_PRODUCT_TREE
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE);


	IF V_PRODUCTTREE_CNT = 0 AND V_PRODUCTTREE_BASE_CNT>0 THEN  

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,V_C_PROD_TYPE||' product tree handling','PRC_SRC_PRODUCT_TREE_UPDATE','Loading the loading the default products  for gcp RM in to the product tree table','NEW', v_log_message, SYSDATE ); -- Updated by Paul on 13-Apr-22

INSERT INTO NY_MIG_PRODUCT_TREE (
				MAPPING_TYPE,
				PRODUCT_TYPE, 
				PRODUCT_ID,
				S_CODE,
				NAME,
				LINK_TO_SERVICE_POINT,
				CONTRACT_TERM,
				PARENT_S_CODE,
				PARENT,
				RULES,
				PRODUCT_FAMILY,
                ATTRIBUTE_TYPE,
                ATTRIBUTE_DISPLAY_NAME,
                UNITPRICE,
                DVCPE_DEVICE_QUANTITY,
                DISCOUNTAMOUNT,
                PRODUCTTERM,
                PRODUCTTERMUNIT,
				FILE_ID,
				ORD_PROD_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				APPOINTMENT_REFERENCE__C,
				APPOINTMENTENDDATE,
				APPOINTMENTSTATUS,
				APPOINTMENTSTARTDATE,
				APPOINTMENTCREATEDDATE,
				APPOINTMENTPRODUCTTYPE,
				VLOCITY_CMT__ONETIMECHARGE__C,
				VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
				PRODUCTNAME,
				VLOCITY_CMT__LINENUMBER__C,
				VLOCITY_CMT__FULFILMENTSTATUS__C,
				VLOCITY_CMT__ISPONRREACHED__C,
				VLOCITY_CMT__PROVISIONINGSTATUS__C,
				PRODUCTCODE,
				PRODUCTID,
				NG_CUSTOMERREQUESTEDDATE__C,
				VLOCITY_CMT__RECURRINGCHARGE__C,
				QUANTITY,
				VLOCITY_CMT__RECURRINGTOTAL__C,
				ORDERITEMNUMBER,
				VLOCITY_CMT__ACTION__C,
                COM_CALC_RECURRINGCHARGE__C,
                DELIVERY_INSTRUCTIONS__C,
                APPOINTMENTENGINEER_INSTRUCTIONS__C,
                APPOINTMENTDELIVERY_PHONE__C,
				STATUS ,
                VLOCITY_CMT__SUBACTION__C,
                OV_ACTION  ,                    /* Added by bala for N 2 N regrade */	
                FULFILLMENTINSTRUCTION,         /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                CEASE_DATE                      /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
				)
SELECT 
				AM.MAPPING_TYPE,
				AM.PRODUCT_TYPE,
				OP.PRODUCTCODE AS PRODUCT_ID,
				AM.S_CODE,
				AM.NAME,
				AM.LINK_TO_SERVICE_POINT,
				AM.CONTRACT_TERM,
				AM.PARENT_S_CODE,
				AM.PARENT,
				AM.RULES,
				AM.PRODUCT_FAMILY,
                AM.ATTRIBUTE_TYPE,
                AM.ATTRIBUTE_DISPLAY_NAME,
                (CASE WHEN INSTR ( ',' || AM.RULES || ','  , ',7,'  ) >0 THEN '[FM.recurringPrice]' 
                        WHEN INSTR (',' || AM.RULES || ','  , ',8,'  ) >0 THEN '[FM.onetimePrice]'
                        WHEN INSTR (',' || AM.RULES || ','  , ',13,'  ) >0 THEN '[FM.onetimePrice]' ELSE NULL END) AS unitPrice, /* SFI-31718 - BB Unit price issue - Added By Elen on 23/Jun/2022 */
                '' DVCPE_DEVICE_QUANTITY,
                '' DISCOUNTAMOUNT,
                '' PRODUCTTERM,
                '' PRODUCTTERMUNIT,
			OP.FILE_ID,
			OP.ORD_PROD_ID,
			OP.ORDERNUMBER,
			OP.BILLINGACCOUNTNO,
			OP.APPOINTMENT_REFERENCE__C,
			OP.APPOINTMENTENDDATE,
			OP.APPOINTMENTSTATUS,
			OP.APPOINTMENTSTARTDATE,
			OP.APPOINTMENTCREATEDDATE,
			OP.APPOINTMENTPRODUCTTYPE,
			OP.VLOCITY_CMT__ONETIMECHARGE__C,
			OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
			OP.PRODUCTNAME,
			OP.VLOCITY_CMT__LINENUMBER__C,
			OP.VLOCITY_CMT__FULFILMENTSTATUS__C,
			OP.VLOCITY_CMT__ISPONRREACHED__C,
			OP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
			OP.PRODUCTCODE,
			OP.PRODUCTID,
			OP.NG_CUSTOMERREQUESTEDDATE__C,
			OP.VLOCITY_CMT__RECURRINGCHARGE__C,
			OP.QUANTITY,
			OP.VLOCITY_CMT__RECURRINGTOTAL__C,
			OP.ORDERITEMNUMBER,
			OP.VLOCITY_CMT__ACTION__C,
            OP.COM_CALC_RECURRINGCHARGE__C,
            OP.DELIVERY_INSTRUCTIONS__C,
            OP.APPOINTMENTENGINEER_INSTRUCTIONS__C,
            OP.APPOINTMENTDELIVERY_PHONE__C,
			OP.STATUS,
            OP.VLOCITY_CMT__SUBACTION__C,
			 'Add' as OV_ACTION ,                     
             OP.FULFILLMENTINSTRUCTION,
             OP.CEASE_DATE
			FROM
				NY_MIG_ORDER_PRODUCT OP,
				DRIVER_ASSET_MAPPING AM
			WHERE
			 OP.FILE_ID = V_FILE_ID 
            AND UPPER(OP.VLOCITY_CMT__ACTION__C) <> UPPER('Disconnect')    -- Added for N2N regrade scenario
			AND UPPER(AM.MAPPING_TYPE) <> 'REDUNDANT'
			AND UPPER(AM.PRODUCT_ID) = UPPER(('v_'||V_B_PROD_TYPE))
            AND UPPER(AM.PRODUCT_TYPE) NOT IN  UPPER(V_D_PROD_TYPE)
            AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OP.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER(V_B_PROD_TYPE));
			COMMIT;
    END IF;
    END IF;

            /*Code changes added by raju palle on 22-08-22 for GCP Reverse mapping ENDS*/

  /*TVBRITBOX logic Added by RUDHRA KUMAR ON 29-JUL-22  ::STARTS*/
/*
IF Regexp_count(v_rules,'34') > 0 AND C.PRODUCT_ID = 'N0001313' THEN

    IF V_IS_REGRADE = 'N' THEN

         DELETE 
         FROM NY_MIG_PRODUCT_TREE
         WHERE FILE_ID = V_FILE_ID
         AND S_CODE <> 'S0551944'
         AND PRODUCT_ID = 'N0001313';
             commit;

    ELSIF V_IS_REGRADE = 'Y' THEN

    SELECT (CASE WHEN FULFILLMENTINSTRUCTION = 'Subscribe' THEN 1
                WHEN FULFILLMENTINSTRUCTION = 'AutoRenewOn' THEN 2
                WHEN FULFILLMENTINSTRUCTION = 'AutoRenewOff' AND CEASE_DATE IS NOT NULL THEN 3
                ELSE 0 END) 
                INTO V_ORDER_PRD_CNT
                FROM NY_MIG_ORDER_PRODUCT
                WHERE FILE_ID = V_FILE_ID
                AND PRODUCTCODE = 'N0001313';      

     SELECT COUNT(*)
                INTO  V_ORDER_TMF_CNT
                FROM  NY_MIG_TMF_ORDERPRODUCT 
                WHERE PRODUCTSPECIFICATIONID IN ('S0507144','S0507145')
                AND   FILE_ID = V_FILE_ID; 

        IF V_ORDER_TMF_CNT = 0 AND V_ORDER_PRD_CNT = 1 THEN

                    DELETE  
                    FROM NY_MIG_PRODUCT_TREE
                    WHERE FILE_ID = V_FILE_ID
                    AND S_CODE <> 'S0551944'
                    AND PRODUCT_ID = 'N0001313';
                    commit; 

        ELSIF        
            ( V_ORDER_TMF_CNT > 0 AND ( V_ORDER_PRD_CNT = 2 OR V_ORDER_PRD_CNT = 3 )) AND C.S_CODE <> 'S0551944' THEN


              DELETE  
                    FROM NY_MIG_PRODUCT_TREE
                    WHERE FILE_ID = V_FILE_ID
                    AND S_CODE <> 'S0551944'
                    AND PRODUCT_ID = 'N0001313';
                    commit;  

        ELSIF    
           ( V_ORDER_TMF_CNT > 0 AND V_ORDER_PRD_CNT > 0)  THEN       

               SELECT COUNT(*) OVER (),PRODUCTSPECIFICATIONID
                    INTO  V_ORDER_TMF_CNT2 , V_ORDER_TMF_VAL
                    FROM  NY_MIG_TMF_ORDERPRODUCT 
                    WHERE PRODUCTSPECIFICATIONID IN ('S0507144','S0507145')
                    AND   FILE_ID = V_FILE_ID; 

            IF V_ORDER_TMF_CNT2 > 0 THEN

             DELETE 
               FROM NY_MIG_PRODUCT_TREE
              WHERE FILE_ID = V_FILE_ID
                  AND S_CODE = V_ORDER_TMF_VAL;
                  --AND PRODUCT_ID = 'N0001313';
             COMMIT;       
            END IF;
        END IF;

    END IF; --Is_regrade ends

END IF; -- Rule 34 ends
*/
  /*TVBRITBOX logic Added by RUDHRA KUMAR ON 29-JUL-22  ::ENDS*/

 /*APPLE TV logic Added by RUDHRA KUMAR ON 29-JUL-22  ::STARTS*/
/*
 IF Regexp_count(v_rules,'7') > 0 AND C.PRODUCT_ID = 'N0001036' THEN

SELECT count(*)	
       INTO V_APP_BRIT_VAL
		FROM NY_MIG_PRODUCT_TREE 
        WHERE FILE_ID = V_FILE_ID
        and PRODUCT_ID = 'N0001036' ;

  IF V_APP_BRIT_VAL > 0 THEN

	IF V_IS_REGRADE = 'N' THEN

         DELETE 
         FROM NY_MIG_PRODUCT_TREE
         WHERE FILE_ID = V_FILE_ID
         AND S_CODE <> 'S0551944'
         AND PRODUCT_ID = 'N0001036';
             commit;

    ELSIF V_IS_REGRADE = 'Y' THEN

    SELECT (CASE WHEN FULFILLMENTINSTRUCTION IS NULL THEN 1
                WHEN FULFILLMENTINSTRUCTION = 'AutoRenewOn' THEN 2
                WHEN FULFILLMENTINSTRUCTION = 'AutoRenewOff' AND CEASE_DATE IS NOT NULL THEN 3
            END) 
                INTO V_ORDER_PRD_APP_CNT
                FROM NY_MIG_ORDER_PRODUCT
                WHERE FILE_ID = V_FILE_ID
                AND PRODUCTCODE = 'N0001036';      

     SELECT COUNT(*)
                INTO  V_ORDER_TMF_APP_CNT
                FROM  NY_MIG_TMF_ORDERPRODUCT 
                WHERE PRODUCTSPECIFICATIONID IN ('S0573304','S0578325','S0578324','S0578304')
                AND   FILE_ID = V_FILE_ID; 

        IF V_ORDER_TMF_APP_CNT = 0 AND V_ORDER_PRD_APP_CNT = 1 THEN

                    DELETE  
                    FROM NY_MIG_PRODUCT_TREE
                    WHERE FILE_ID = V_FILE_ID
                    AND S_CODE <> 'S0551944'
                    AND PRODUCT_ID = 'N0001036';
                    commit; 

        ELSIF        
            ( V_ORDER_TMF_APP_CNT > 0 AND ( V_ORDER_PRD_APP_CNT = 2 OR V_ORDER_PRD_APP_CNT = 3 )) AND C.S_CODE <> 'S0551944' THEN

              DELETE  
                    FROM NY_MIG_PRODUCT_TREE
                    WHERE FILE_ID = V_FILE_ID
                    AND S_CODE <> 'S0551944'
                    AND PRODUCT_ID = 'N0001036';
                    commit;  

        ELSIF    
           ( V_ORDER_TMF_APP_CNT > 0 AND V_ORDER_PRD_APP_CNT = 1)  THEN       

               SELECT COUNT(*) OVER (),PRODUCTSPECIFICATIONID
                    INTO  V_ORDER_TMF_APP_CNT2 , V_ORDER_TMF_APP_VAL
                    FROM  NY_MIG_TMF_ORDERPRODUCT 
                    WHERE PRODUCTSPECIFICATIONID IN ('S0573304','S0578325','S0578324','S0578304')
                    AND   FILE_ID = V_FILE_ID; 

            IF V_ORDER_TMF_APP_CNT2 > 0 THEN

             DELETE 
               FROM NY_MIG_PRODUCT_TREE
              WHERE FILE_ID = V_FILE_ID
                  AND S_CODE <> V_ORDER_TMF_APP_VAL
                  AND PRODUCT_ID = 'N0001036';
             COMMIT;       
            END IF;
        END IF;
	END IF;	
 END IF;
END IF; -- Rule 34 ends

*/
  /*APPLE TV  logic Added by RUDHRA KUMAR ON 29-JUL-22  ::ENDS*/


/*-- Added by Rudhra Kumar/Raju Palle on 17-JUN-22 ::Managed Install ::Starts*/

IF Regexp_count(v_rules,'33') > 0 THEN

                        SELECT COUNT (*)
                        INTO V_ORDER_ATTR_CNT
                        FROM NY_MIG_ORDER_ATTRIBUTES MOT
                        WHERE MOT.FILE_ID = V_FILE_ID 
                        AND UPPER(ATTRIBUTENAME)= 'VEPC_ATTR_RT_SITE_VISIT_REASON'
                        AND MOT.PRODUCTCODE='N0001093'
                        AND REPLACE(ATTRIBUTEVALUE,' ','') IS NOT NULL
                        AND EXISTS (SELECT 1
                        FROM NY_MIG_PRODUCT_TREE MPT
                        WHERE MPT.ORDERNUMBER= MOT.ORDERNUMBER
                        AND MPT.PRODUCTCODE = MOT.PRODUCTCODE);

        IF V_ORDER_ATTR_CNT>0 THEN

                        DELETE FROM NY_MIG_PRODUCT_TREE NMP
                        WHERE NMP.FILE_ID = V_FILE_ID 
                        and PRODUCT_id='N0001093'
                        AND NMP.NAME NOT IN (SELECT ATTRIBUTEVALUE FROM
                        (WITH DATA AS
                        ( SELECT ATTRIBUTEVALUE
                        FROM NY_MIG_ORDER_ATTRIBUTES WHERE PRODUCTCODE='N0001093' AND FILE_ID = V_FILE_ID 
                        AND ATTRIBUTENAME='VEPC_ATTR_RT_SITE_VISIT_REASON')
                        SELECT trim(regexp_substr(ATTRIBUTEVALUE, '[^;]+', 1, LEVEL)) ATTRIBUTEVALUE
                        FROM DATA
                        CONNECT BY LEVEL <= regexp_count(ATTRIBUTEVALUE, ';')+1));

         ELSIF V_ORDER_ATTR_CNT = 0 THEN 

                            DELETE  FROM NY_MIG_PRODUCT_TREE NMP
                            WHERE  NMP.FILE_ID = V_FILE_ID  
                            AND PRODUCT_id='N0001093';      
                            END IF;


END IF;        

 /*-- Added by Rudhra Kumar/Raju Palle on 17-JUN-22 ::Managed Install ::End*/


/* Code changes ADDED for CNFs to be defaulted based on Call Plans for voice 09-MAY-22 by RUDHRAKUMAR NAGARAJAN :: STARTS */

IF UPPER(C.PRODUCT_TYPE) = UPPER('VoiceBase') THEN 

		PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'DVCNF product tree handling','PRC_SRC_PRODUCT_TREE_UPDATE','Loading the contract bundle into the product tree table','NEW', v_log_message, SYSDATE );

            INSERT INTO NY_MIG_PRODUCT_TREE (
				MAPPING_TYPE,
				PRODUCT_TYPE,
				PRODUCT_ID,
				S_CODE,
				NAME,
				LINK_TO_SERVICE_POINT,
				CONTRACT_TERM,
				PARENT_S_CODE,
				PARENT,
				RULES,
				PRODUCT_FAMILY,
                ATTRIBUTE_TYPE,
                ATTRIBUTE_DISPLAY_NAME,
                UNITPRICE,
                DVCPE_DEVICE_QUANTITY,
                DISCOUNTAMOUNT,
                PRODUCTTERM,
                PRODUCTTERMUNIT,
				FILE_ID,
				ORD_PROD_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				APPOINTMENT_REFERENCE__C,
				APPOINTMENTENDDATE,
				APPOINTMENTSTATUS,
				APPOINTMENTSTARTDATE,
				APPOINTMENTCREATEDDATE,
				APPOINTMENTPRODUCTTYPE,
				VLOCITY_CMT__ONETIMECHARGE__C,
				VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
				PRODUCTNAME,
				VLOCITY_CMT__LINENUMBER__C,
				VLOCITY_CMT__FULFILMENTSTATUS__C,
				VLOCITY_CMT__ISPONRREACHED__C,
				VLOCITY_CMT__PROVISIONINGSTATUS__C,
				PRODUCTCODE,
				PRODUCTID,
				NG_CUSTOMERREQUESTEDDATE__C,
				VLOCITY_CMT__RECURRINGCHARGE__C,
				QUANTITY,
				VLOCITY_CMT__RECURRINGTOTAL__C,
				ORDERITEMNUMBER,
				VLOCITY_CMT__ACTION__C,
                COM_CALC_RECURRINGCHARGE__C,
                DELIVERY_INSTRUCTIONS__C,
                APPOINTMENTENGINEER_INSTRUCTIONS__C,
                APPOINTMENTDELIVERY_PHONE__C,
				STATUS,
                VLOCITY_CMT__SUBACTION__C,
                OV_ACTION ,                     /* Added by bala for N 2 N regrade */		
                FULFILLMENTINSTRUCTION,         /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                CEASE_DATE                      /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
				)

SELECT 
				AM.MAPPING_TYPE,
				AM.PRODUCT_TYPE,
				OP.PRODUCTCODE AS PRODUCT_ID,
				AM.S_CODE,
				AM.NAME,
				AM.LINK_TO_SERVICE_POINT,
				AM.CONTRACT_TERM,
				AM.PARENT_S_CODE,
				AM.PARENT,
				AM.RULES,
				AM.PRODUCT_FAMILY,
                AM.ATTRIBUTE_TYPE,
                AM.ATTRIBUTE_DISPLAY_NAME,
				(CASE WHEN INSTR ( ',' || AM.RULES || ','  , ',7,'  ) >0 THEN '[FM.recurringPrice]' 
                        WHEN INSTR (',' || AM.RULES || ','  , ',8,'  ) >0 THEN '[FM.onetimePrice]'
                        WHEN INSTR (',' || AM.RULES || ','  , ',13,'  ) >0 THEN '[FM.onetimePrice]' ELSE NULL END) AS unitPrice, /* SFI-31718 - BB Unit price issue - Added By Elen on 23/Jun/2022 */
                '' DVCPE_DEVICE_QUANTITY,
                '' DISCOUNTAMOUNT,
                '' PRODUCTTERM,
                '' PRODUCTTERMUNIT,
			OP.FILE_ID,
			OP.ORD_PROD_ID,
			OP.ORDERNUMBER,
			OP.BILLINGACCOUNTNO,
			OP.APPOINTMENT_REFERENCE__C,
			OP.APPOINTMENTENDDATE,
			OP.APPOINTMENTSTATUS,
			OP.APPOINTMENTSTARTDATE,
			OP.APPOINTMENTCREATEDDATE,
			OP.APPOINTMENTPRODUCTTYPE,
			OP.VLOCITY_CMT__ONETIMECHARGE__C,
			OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
			OP.PRODUCTNAME,
			OP.VLOCITY_CMT__LINENUMBER__C,
			OP.VLOCITY_CMT__FULFILMENTSTATUS__C,
			OP.VLOCITY_CMT__ISPONRREACHED__C,
			OP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
			OP.PRODUCTCODE,
			OP.PRODUCTID,
			OP.NG_CUSTOMERREQUESTEDDATE__C,
			OP.VLOCITY_CMT__RECURRINGCHARGE__C,
			OP.QUANTITY,
			OP.VLOCITY_CMT__RECURRINGTOTAL__C,
			OP.ORDERITEMNUMBER,
			OP.VLOCITY_CMT__ACTION__C,
            OP.COM_CALC_RECURRINGCHARGE__C,
            OP.DELIVERY_INSTRUCTIONS__C,
            OP.APPOINTMENTENGINEER_INSTRUCTIONS__C,
            OP.APPOINTMENTDELIVERY_PHONE__C,
			OP.STATUS,
            OP.VLOCITY_CMT__SUBACTION__C,
			 'Add' as OV_ACTION ,                     /* Added by bala for N 2 N regrade */
             OP.FULFILLMENTINSTRUCTION,               /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
             OP.CEASE_DATE                            /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
			FROM
				NY_MIG_ORDER_PRODUCT OP,
				DRIVER_ASSET_MAPPING AM

			WHERE
			    OP.FILE_ID = V_FILE_ID 
            AND UPPER(OP.VLOCITY_CMT__ACTION__C) <> UPPER('Disconnect')    -- Added for N2N regrade scenario
			AND UPPER(AM.MAPPING_TYPE) <> 'REDUNDANT'
			AND UPPER(AM.PRODUCT_ID) = UPPER('v_VoiceBase')
			AND UPPER(AM.PRODUCT_TYPE) IN  UPPER('DVCNF')
            AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OP.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('VoiceBase'));


			COMMIT;

END IF; -- CNFs product tree validation ends

/* Code changes ADDED for CNFs to be defaulted based on Call Plans for voice 09-MAY-22 by RUDHRAKUMAR NAGARAJAN :: ENDS */

dbms_output.put_line('before cnf');

/* Code changes ADDED for DV-PIN SERVICE to be defaulted based on DVCNF for voice 18-MAY-22 by RUDHRAKUMAR NAGARAJAN :: STARTS */

IF (UPPER(C.PRODUCT_TYPE)= upper('DVService')) THEN  

    SELECT PAR_VAL
    INTO V_CNF_PAR_VAL
    FROM NAYAN_CONFIG_PARAMS
    WHERE UPPER(PAR_TYPE) = UPPER(23)
	AND UPPER(PAR_NAME)=UPPER('DVCNF');	

	SELECT PAR_VAL
    INTO V_ATT_PAR_VAL
    FROM NAYAN_CONFIG_PARAMS
    WHERE UPPER(PAR_TYPE) = UPPER(23)
	AND UPPER(PAR_NAME)=UPPER('ATTRIBUTE');	

	SELECT COUNT (1) 
    INTO V_ORDER_ATTR_CNT
    FROM NY_MIG_ORDER_ATTRIBUTES
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(TRIM(ATTRIBUTENAME)) = V_ATT_PAR_VAL
	AND ATTRIBUTEVALUE IS NOT NULL;

    SELECT COUNT (1)
    INTO V_PRODUCTTREE_CNT
    FROM NY_MIG_PRODUCT_TREE
    WHERE FILE_ID = V_FILE_ID 
	AND UPPER(PRODUCT_ID) = UPPER(V_CNF_PAR_VAL)
    AND UPPER(PRODUCTNAME) = UPPER('Temporary PIN');

dbms_output.put_line('before cnf insert');


	IF V_ORDER_ATTR_CNT > 0 AND V_PRODUCTTREE_CNT = 0 THEN
		PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,V_C_PROD_TYPE||' product tree handling','PRC_SRC_PRODUCT_TREE_UPDATE','Loading the contract bundle into the product tree table','NEW', v_log_message, SYSDATE ); 

            INSERT INTO NY_MIG_PRODUCT_TREE (
				MAPPING_TYPE,
				PRODUCT_TYPE,
				PRODUCT_ID,
				S_CODE,
				NAME,
				LINK_TO_SERVICE_POINT,
				CONTRACT_TERM,
				PARENT_S_CODE,
				PARENT,
				RULES,
				PRODUCT_FAMILY,
                ATTRIBUTE_TYPE,
                ATTRIBUTE_DISPLAY_NAME,
                UNITPRICE,
                DVCPE_DEVICE_QUANTITY,
                DISCOUNTAMOUNT,
                PRODUCTTERM,
                PRODUCTTERMUNIT,
				FILE_ID,
				ORD_PROD_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				APPOINTMENT_REFERENCE__C,
				APPOINTMENTENDDATE,
				APPOINTMENTSTATUS,
				APPOINTMENTSTARTDATE,
				APPOINTMENTCREATEDDATE,
				APPOINTMENTPRODUCTTYPE,
				VLOCITY_CMT__ONETIMECHARGE__C,
				VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
				PRODUCTNAME,
				VLOCITY_CMT__LINENUMBER__C,
				VLOCITY_CMT__FULFILMENTSTATUS__C,
				VLOCITY_CMT__ISPONRREACHED__C,
				VLOCITY_CMT__PROVISIONINGSTATUS__C,
				PRODUCTCODE,
				PRODUCTID,
				NG_CUSTOMERREQUESTEDDATE__C,
				VLOCITY_CMT__RECURRINGCHARGE__C,
				QUANTITY,
				VLOCITY_CMT__RECURRINGTOTAL__C,
				ORDERITEMNUMBER,
				VLOCITY_CMT__ACTION__C,
                COM_CALC_RECURRINGCHARGE__C,
                DELIVERY_INSTRUCTIONS__C,
                APPOINTMENTENGINEER_INSTRUCTIONS__C,
                APPOINTMENTDELIVERY_PHONE__C,
				STATUS   ,
                VLOCITY_CMT__SUBACTION__C,
                OV_ACTION ,                     /* Added by bala for N 2 N regrade */		
                FULFILLMENTINSTRUCTION, /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                CEASE_DATE              /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
				)

SELECT 
				AM.MAPPING_TYPE,
				AM.PRODUCT_TYPE,
				OP.PRODUCTCODE AS PRODUCT_ID,
				AM.S_CODE,
				AM.NAME,
				AM.LINK_TO_SERVICE_POINT,
				AM.CONTRACT_TERM,
				AM.PARENT_S_CODE,
				AM.PARENT,
				AM.RULES,
				AM.PRODUCT_FAMILY,
                AM.ATTRIBUTE_TYPE,
                AM.ATTRIBUTE_DISPLAY_NAME,
                (CASE WHEN INSTR ( ',' || AM.RULES || ','  , ',7,'  ) >0 THEN '[FM.recurringPrice]' 
                        WHEN INSTR (',' || AM.RULES || ','  , ',8,'  ) >0 THEN '[FM.onetimePrice]'
                        WHEN INSTR (',' || AM.RULES || ','  , ',13,'  ) >0 THEN '[FM.onetimePrice]' ELSE NULL END) AS unitPrice, /* SFI-31718 - BB Unit price issue - Added By Elen on 23/Jun/2022 */
				'' DVCPE_DEVICE_QUANTITY,
                '' DISCOUNTAMOUNT,
                '' PRODUCTTERM,
                '' PRODUCTTERMUNIT,
			OP.FILE_ID,
			OP.ORD_PROD_ID,
			OP.ORDERNUMBER,
			OP.BILLINGACCOUNTNO,
			OP.APPOINTMENT_REFERENCE__C,
			OP.APPOINTMENTENDDATE,
			OP.APPOINTMENTSTATUS,
			OP.APPOINTMENTSTARTDATE,
			OP.APPOINTMENTCREATEDDATE,
			OP.APPOINTMENTPRODUCTTYPE,
			OP.VLOCITY_CMT__ONETIMECHARGE__C,
			OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
			AM.PRODUCT_NAME,
			OP.VLOCITY_CMT__LINENUMBER__C,
			OP.VLOCITY_CMT__FULFILMENTSTATUS__C,
			OP.VLOCITY_CMT__ISPONRREACHED__C,
			OP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
			OP.PRODUCTCODE,
			OP.PRODUCTID,
			OP.NG_CUSTOMERREQUESTEDDATE__C,
			OP.VLOCITY_CMT__RECURRINGCHARGE__C,
			OP.QUANTITY,
			OP.VLOCITY_CMT__RECURRINGTOTAL__C,
			OP.ORDERITEMNUMBER,
			OP.VLOCITY_CMT__ACTION__C,
            OP.COM_CALC_RECURRINGCHARGE__C,
            OP.DELIVERY_INSTRUCTIONS__C,
            OP.APPOINTMENTENGINEER_INSTRUCTIONS__C,
            OP.APPOINTMENTDELIVERY_PHONE__C,
			OP.STATUS,
            OP.VLOCITY_CMT__SUBACTION__C,
			 'Add' as OV_ACTION   ,                   /* Added by bala for N 2 N regrade */
             OP.FULFILLMENTINSTRUCTION,               /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
             OP.CEASE_DATE                            /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
			FROM
				NY_MIG_ORDER_PRODUCT OP,
				DRIVER_ASSET_MAPPING AM,
                (SELECT * FROM NY_MIG_ORDER_ATTRIBUTES OA WHERE OA.FILE_ID = V_FILE_ID  AND UPPER(OA.ATTRIBUTENAME) = UPPER(V_ATT_PAR_VAL)
                 AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('DVService'))) OA -- Updated by RUDHRA KUMAR on 18-MAY-22
			WHERE
			OP.ORDERNUMBER = OA.ORDERNUMBER
			AND OP.PRODUCTCODE = OA.PRODUCTCODE
			AND OP.FILE_ID = V_FILE_ID 
            AND UPPER(OP.VLOCITY_CMT__ACTION__C) <> UPPER('Disconnect')    -- Added for N2N regrade scenario
			AND UPPER(AM.MAPPING_TYPE) <> 'REDUNDANT'
			AND UPPER(AM.PRODUCT_ID) = UPPER('v_PIN')
            AND UPPER(AM.PRODUCT_TYPE) IN UPPER('DVCNF');

   END IF; 

END IF; -- DV-PIN SERVICE product tree validation ends

/* Code changes ADDED for DV-PIN SERVICE to be defaulted based on DVCNF for voice 18-MAY-22 by RUDHRAKUMAR NAGARAJAN :: ENDS */

dbms_output.put_line('after cnf insert');


/* Code changes ADDED for DV CNFs Transform logic 23-Aug-22 by Kanagavel s :: STARTS */
IF UPPER(C.PRODUCT_TYPE) = UPPER('VoiceBase') THEN 

        SELECT COUNT (*)
                        INTO V_ORDER_ATTR_CNT
                        FROM NY_MIG_ORDER_ATTRIBUTES MOT
                        WHERE MOT.FILE_ID = V_FILE_ID 
                        AND UPPER(ATTRIBUTENAME)= 'VEPC_ATTR_DT_CALL_FEATURES'
                        AND MOT.PRODUCTCODE=C.PRODUCT_ID
                        AND REPLACE(ATTRIBUTEVALUE,' ','') IS NOT NULL
                        AND EXISTS (SELECT 1
                        FROM NY_MIG_PRODUCT_TREE MPT
                        WHERE MPT.ORDERNUMBER= MOT.ORDERNUMBER
                        AND MPT.PRODUCTCODE = MOT.PRODUCTCODE);

IF V_ORDER_ATTR_CNT>0 THEN

	   DELETE FROM NY_MIG_PRODUCT_TREE NMP
                        WHERE NMP.FILE_ID = V_FILE_ID 
                        AND PRODUCT_TYPE ='DVCNF'
                        and PRODUCT_ID= C.PRODUCT_ID
						AND NMP.S_CODE NOT IN                                              
                       ( SELECT PAR_VAL   FROM
                        (WITH DATA AS
                        ( SELECT ATTRIBUTEVALUE
                        FROM NY_MIG_ORDER_ATTRIBUTES 
						WHERE FILE_ID = V_FILE_ID 
                        AND ATTRIBUTENAME='VEPC_ATTR_DT_CALL_FEATURES')
                        SELECT trim(regexp_substr(ATTRIBUTEVALUE, '[^;]+', 1, LEVEL)) ATTRIBUTEVALUE
                        FROM DATA
                        CONNECT BY LEVEL <= regexp_count(ATTRIBUTEVALUE, ';')+1) ,NAYAN_CONFIG_PARAMS
                        WHERE PAR_TYPE = 'DVCNF'
                        AND PAR_NAME = ATTRIBUTEVALUE
                      );

                        commit;
ELSIF V_ORDER_ATTR_CNT = 0 THEN 

	  DELETE  FROM NY_MIG_PRODUCT_TREE NMP
                         WHERE  NMP.FILE_ID = V_FILE_ID 
                         AND PRODUCT_ID= C.PRODUCT_ID; 
                         commit;                        
						 END IF;

END IF;						

/* Code changes ADDED for DV CNFs Transform logic 23-Aug-22 by Kanagavel s :: ENDS */

/* Code changes ADDED for Sports on app logic 29-Aug-22 by Kanagavel s :: starts */

IF UPPER(C.PRODUCT_TYPE) = UPPER('SportBase') THEN

SELECT COUNT(1)
INTO V_PRODUCTTREE_CNT
FROM NY_MIG_PRODUCT_TREE P
WHERE FILE_ID = V_FILE_ID 
AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE UPPER(AM1.PRODUCT_ID) = UPPER('v_SportBase') 
AND UPPER(AM1.PRODUCT_TYPE) NOT IN ('SPORTDISCOUNT','SPORTCONTRACT')
AND AM1.S_CODE = P.S_CODE);

	IF V_PRODUCTTREE_CNT = 0 THEN
		PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'MultiPlay product tree handling','PRC_SRC_PRODUCT_TREE_UPDATE','Loading the contract bundle into the product tree table','NEW', v_log_message, SYSDATE );

            INSERT INTO NY_MIG_PRODUCT_TREE (
				MAPPING_TYPE,
				PRODUCT_TYPE,
				PRODUCT_ID,
				S_CODE,
				NAME,
				LINK_TO_SERVICE_POINT,
				CONTRACT_TERM,
				PARENT_S_CODE,
				PARENT,
				RULES,
				PRODUCT_FAMILY,
                ATTRIBUTE_TYPE,
                ATTRIBUTE_DISPLAY_NAME,
                UNITPRICE,
                DVCPE_DEVICE_QUANTITY,
                DISCOUNTAMOUNT,
                PRODUCTTERM,
                PRODUCTTERMUNIT,
				FILE_ID,
				ORD_PROD_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				APPOINTMENT_REFERENCE__C,
				APPOINTMENTENDDATE,
				APPOINTMENTSTATUS,
				APPOINTMENTSTARTDATE,
				APPOINTMENTCREATEDDATE,
				APPOINTMENTPRODUCTTYPE,
				VLOCITY_CMT__ONETIMECHARGE__C,
				VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
				PRODUCTNAME,
				VLOCITY_CMT__LINENUMBER__C,
				VLOCITY_CMT__FULFILMENTSTATUS__C,
				VLOCITY_CMT__ISPONRREACHED__C,
				VLOCITY_CMT__PROVISIONINGSTATUS__C,
				PRODUCTCODE,
				PRODUCTID,
				NG_CUSTOMERREQUESTEDDATE__C,
				VLOCITY_CMT__RECURRINGCHARGE__C,
				QUANTITY,
				VLOCITY_CMT__RECURRINGTOTAL__C,
				ORDERITEMNUMBER,
				VLOCITY_CMT__ACTION__C,
                COM_CALC_RECURRINGCHARGE__C,
                DELIVERY_INSTRUCTIONS__C,
                APPOINTMENTENGINEER_INSTRUCTIONS__C,
                APPOINTMENTDELIVERY_PHONE__C,
				STATUS ,
                VLOCITY_CMT__SUBACTION__C,
                OV_ACTION  ,                    /* Added by bala for N 2 N regrade */	
                FULFILLMENTINSTRUCTION,         /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                CEASE_DATE                      /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
				)

SELECT 
				AM.MAPPING_TYPE,
				AM.PRODUCT_TYPE,
				OP.PRODUCTCODE AS PRODUCT_ID,
				AM.S_CODE,
				AM.NAME,
				AM.LINK_TO_SERVICE_POINT,
				AM.CONTRACT_TERM,
				AM.PARENT_S_CODE,
				AM.PARENT,
				AM.RULES,
				AM.PRODUCT_FAMILY,
                AM.ATTRIBUTE_TYPE,
                AM.ATTRIBUTE_DISPLAY_NAME,
                (CASE WHEN INSTR ( ',' || AM.RULES || ','  , ',7,'  ) >0 THEN '[FM.recurringPrice]' 
                        WHEN INSTR (',' || AM.RULES || ','  , ',8,'  ) >0 THEN '[FM.onetimePrice]'
                        WHEN INSTR (',' || AM.RULES || ','  , ',13,'  ) >0 THEN '[FM.onetimePrice]' ELSE NULL END) AS unitPrice, /* SFI-31718 - BB Unit price issue - Added By Elen on 23/Jun/2022 */
                '' DVCPE_DEVICE_QUANTITY,
                '' DISCOUNTAMOUNT,
                '' PRODUCTTERM,
                '' PRODUCTTERMUNIT,
			OP.FILE_ID,
			OP.ORD_PROD_ID,
			OP.ORDERNUMBER,
			OP.BILLINGACCOUNTNO,
			OP.APPOINTMENT_REFERENCE__C,
			OP.APPOINTMENTENDDATE,
			OP.APPOINTMENTSTATUS,
			OP.APPOINTMENTSTARTDATE,
			OP.APPOINTMENTCREATEDDATE,
			OP.APPOINTMENTPRODUCTTYPE,
			OP.VLOCITY_CMT__ONETIMECHARGE__C,
			OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
			OP.PRODUCTNAME,
			OP.VLOCITY_CMT__LINENUMBER__C,
			OP.VLOCITY_CMT__FULFILMENTSTATUS__C,
			OP.VLOCITY_CMT__ISPONRREACHED__C,
			OP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
			OP.PRODUCTCODE,
			OP.PRODUCTID,
			OP.NG_CUSTOMERREQUESTEDDATE__C,
			OP.VLOCITY_CMT__RECURRINGCHARGE__C,
			OP.QUANTITY,
			OP.VLOCITY_CMT__RECURRINGTOTAL__C,
			OP.ORDERITEMNUMBER,
			OP.VLOCITY_CMT__ACTION__C,
            OP.COM_CALC_RECURRINGCHARGE__C,
            OP.DELIVERY_INSTRUCTIONS__C,
            OP.APPOINTMENTENGINEER_INSTRUCTIONS__C,
            OP.APPOINTMENTDELIVERY_PHONE__C,
			OP.STATUS,
            OP.VLOCITY_CMT__SUBACTION__C,
			 'Add' as OV_ACTION ,                     /* Added by bala for N 2 N regrade */
             OP.FULFILLMENTINSTRUCTION,               /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
             OP.CEASE_DATE                            /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
			FROM
				NY_MIG_ORDER_PRODUCT OP,
				DRIVER_ASSET_MAPPING AM,
                DRIVER_ASSET_MANDATORY D
			WHERE
			AM.S_CODE = D.DRIVER_S_CODE(+)
			AND OP.FILE_ID = V_FILE_ID 
            AND UPPER(OP.VLOCITY_CMT__ACTION__C) <> UPPER('Disconnect')    -- Added for N2N regrade scenario
			AND UPPER(AM.MAPPING_TYPE) <> 'REDUNDANT'
			AND UPPER(AM.PRODUCT_ID) = UPPER('v_SportBase')
			AND UPPER(AM.PRODUCT_TYPE) NOT IN  ('SPORTDISCOUNT','SPORTCONTRACT')
            AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OP.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('SportBase'));
			COMMIT;

	END IF;

END IF;
/* Code changes ADDED for Sports on app logic 29-Aug-22 by Kanagavel s :: END */
-- MultiPlay product tree handling Starts
IF UPPER(C.PRODUCT_TYPE) = UPPER('BBBase') THEN

SELECT COUNT(1)
INTO V_PRODUCTTREE_CNT
FROM NY_MIG_PRODUCT_TREE P
WHERE FILE_ID = V_FILE_ID 
AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE UPPER(AM1.PRODUCT_ID) = UPPER('v_BBBase') 
AND UPPER(AM1.PRODUCT_TYPE) NOT IN ('BBDISCOUNT', 'BBCONTRACT')
AND AM1.S_CODE = P.S_CODE
);

	IF V_PRODUCTTREE_CNT = 0 THEN
		PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'MultiPlay product tree handling','PRC_SRC_PRODUCT_TREE_UPDATE','Loading the contract bundle into the product tree table','NEW', v_log_message, SYSDATE );

            INSERT INTO NY_MIG_PRODUCT_TREE (
				MAPPING_TYPE,
				PRODUCT_TYPE,
				PRODUCT_ID,
				S_CODE,
				NAME,
				LINK_TO_SERVICE_POINT,
				CONTRACT_TERM,
				PARENT_S_CODE,
				PARENT,
				RULES,
				PRODUCT_FAMILY,
                ATTRIBUTE_TYPE,
                ATTRIBUTE_DISPLAY_NAME,
                UNITPRICE,
                DVCPE_DEVICE_QUANTITY,
                DISCOUNTAMOUNT,
                PRODUCTTERM,
                PRODUCTTERMUNIT,
				FILE_ID,
				ORD_PROD_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				APPOINTMENT_REFERENCE__C,
				APPOINTMENTENDDATE,
				APPOINTMENTSTATUS,
				APPOINTMENTSTARTDATE,
				APPOINTMENTCREATEDDATE,
				APPOINTMENTPRODUCTTYPE,
				VLOCITY_CMT__ONETIMECHARGE__C,
				VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
				PRODUCTNAME,
				VLOCITY_CMT__LINENUMBER__C,
				VLOCITY_CMT__FULFILMENTSTATUS__C,
				VLOCITY_CMT__ISPONRREACHED__C,
				VLOCITY_CMT__PROVISIONINGSTATUS__C,
				PRODUCTCODE,
				PRODUCTID,
				NG_CUSTOMERREQUESTEDDATE__C,
				VLOCITY_CMT__RECURRINGCHARGE__C,
				QUANTITY,
				VLOCITY_CMT__RECURRINGTOTAL__C,
				ORDERITEMNUMBER,
				VLOCITY_CMT__ACTION__C,
                COM_CALC_RECURRINGCHARGE__C,
                DELIVERY_INSTRUCTIONS__C,
                APPOINTMENTENGINEER_INSTRUCTIONS__C,
                APPOINTMENTDELIVERY_PHONE__C,
				STATUS ,
                VLOCITY_CMT__SUBACTION__C,
                OV_ACTION  ,                    /* Added by bala for N 2 N regrade */	
                FULFILLMENTINSTRUCTION,         /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                CEASE_DATE                      /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
				)

SELECT 
				AM.MAPPING_TYPE,
				AM.PRODUCT_TYPE,
				OP.PRODUCTCODE AS PRODUCT_ID,
				AM.S_CODE,
				AM.NAME,
				AM.LINK_TO_SERVICE_POINT,
				AM.CONTRACT_TERM,
				AM.PARENT_S_CODE,
				AM.PARENT,
				AM.RULES,
				AM.PRODUCT_FAMILY,
                AM.ATTRIBUTE_TYPE,
                AM.ATTRIBUTE_DISPLAY_NAME,
                (CASE WHEN INSTR ( ',' || AM.RULES || ','  , ',7,'  ) >0 THEN '[FM.recurringPrice]' 
                        WHEN INSTR (',' || AM.RULES || ','  , ',8,'  ) >0 THEN '[FM.onetimePrice]'
                        WHEN INSTR (',' || AM.RULES || ','  , ',13,'  ) >0 THEN '[FM.onetimePrice]' ELSE NULL END) AS unitPrice, /* SFI-31718 - BB Unit price issue - Added By Elen on 23/Jun/2022 */
                '' DVCPE_DEVICE_QUANTITY,
                '' DISCOUNTAMOUNT,
                '' PRODUCTTERM,
                '' PRODUCTTERMUNIT,
			OP.FILE_ID,
			OP.ORD_PROD_ID,
			OP.ORDERNUMBER,
			OP.BILLINGACCOUNTNO,
			OP.APPOINTMENT_REFERENCE__C,
			OP.APPOINTMENTENDDATE,
			OP.APPOINTMENTSTATUS,
			OP.APPOINTMENTSTARTDATE,
			OP.APPOINTMENTCREATEDDATE,
			OP.APPOINTMENTPRODUCTTYPE,
			OP.VLOCITY_CMT__ONETIMECHARGE__C,
			OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
			OP.PRODUCTNAME,
			OP.VLOCITY_CMT__LINENUMBER__C,
			OP.VLOCITY_CMT__FULFILMENTSTATUS__C,
			OP.VLOCITY_CMT__ISPONRREACHED__C,
			OP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
			OP.PRODUCTCODE,
			OP.PRODUCTID,
			OP.NG_CUSTOMERREQUESTEDDATE__C,
			OP.VLOCITY_CMT__RECURRINGCHARGE__C,
			OP.QUANTITY,
			OP.VLOCITY_CMT__RECURRINGTOTAL__C,
			OP.ORDERITEMNUMBER,
			OP.VLOCITY_CMT__ACTION__C,
            OP.COM_CALC_RECURRINGCHARGE__C,
            OP.DELIVERY_INSTRUCTIONS__C,
            OP.APPOINTMENTENGINEER_INSTRUCTIONS__C,
            OP.APPOINTMENTDELIVERY_PHONE__C,
			OP.STATUS,
            OP.VLOCITY_CMT__SUBACTION__C,
			 'Add' as OV_ACTION ,                     /* Added by bala for N 2 N regrade */
             OP.FULFILLMENTINSTRUCTION,               /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
             OP.CEASE_DATE                            /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
			FROM
				NY_MIG_ORDER_PRODUCT OP,
				DRIVER_ASSET_MAPPING AM,
                DRIVER_ASSET_MANDATORY D
			WHERE
			AM.S_CODE = D.DRIVER_S_CODE(+)
			AND OP.FILE_ID = V_FILE_ID 
            AND UPPER(OP.VLOCITY_CMT__ACTION__C) <> UPPER('Disconnect')    -- Added for N2N regrade scenario
			AND UPPER(AM.MAPPING_TYPE) <> 'REDUNDANT'
			AND UPPER(AM.PRODUCT_ID) = UPPER('v_BBBase')
			AND UPPER(AM.PRODUCT_TYPE) NOT IN  ('BBDISCOUNT', 'BBCONTRACT')
            AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OP.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('BBBase'));
			COMMIT;

	END IF;

END IF; -- MultiPlay product tree handling Ends


/* Broadband Buyout - Switching Offer Starts */
IF Regexp_count(v_rules,'9') >0 THEN

    /* Check if we have attribute name Broadband Buyout with value as Yes against N0001032 (Broadband Services) in json input, then send the product in xml. */
    SELECT COUNT(1) 
    INTO V_ORDER_ATTR_CNT
    FROM NY_MIG_ORDER_ATTRIBUTES OA
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(ATTRIBUTENAME) = 'VEPC_ATTR_RT_BROADBAND_BUYOUT'
    AND UPPER(ATTRIBUTEVALUE) = 'YES'
    AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('BB'));

    IF V_ORDER_ATTR_CNT = 0 THEN

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Broadband Buyout product tree mapping','PRC_SRC_PRODUCT_TREE_UPDATE','Switching Offer for BB','NEW', v_log_message, SYSDATE );

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND PRODUCT_ID = 'N0001032' AND UPPER(NAME) = UPPER('Switching Offer');
            COMMIT;

    END IF;

END IF; 
/* Broadband Buyout - Switching Offer Ends */

/* NOW ACCOUNT STATUS - NOW Linked Account Marker :: Added by Paul on 13-Apr-22 :: Starts */
IF Regexp_count(v_rules,'11') >0 THEN

    SELECT PAR_NAME, PAR_VAL
    INTO V_NAME, V_PROD_CODE
    FROM NAYAN_CONFIG_PARAMS
    WHERE UPPER(PAR_TYPE) = UPPER('TVVision');

    /* To create the product, check for VEPC_ATTR_NOWACCOUNTSTATUS attribute and its value 'New' or 'Proceedable' against N0001021 (TV Package) product in json input. */
    SELECT COUNT(1) 
    INTO V_ORDER_ATTR_CNT
    FROM NY_MIG_ORDER_ATTRIBUTES OA
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(ATTRIBUTENAME) = 'VEPC_ATTR_NOWACCOUNTSTATUS'
    AND UPPER(ATTRIBUTEVALUE) IN ('NEW', 'PROCEEDABLE')
    AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('TVVision'));

    IF V_ORDER_ATTR_CNT = 0 THEN

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'NOW ACCOUNT STATUS product tree mapping','PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND UPPER(PRODUCT_ID) = UPPER(V_PROD_CODE) AND UPPER(NAME) = UPPER(V_NAME);
            COMMIT;

    END IF;

END IF;
/* NOW ACCOUNT STATUS - NOW Linked Account Marker :: Added by Paul on 13-Apr-22 :: Ends */

/*NUMBER port Scenarios part of C1 :: Added by kanagavel on 18-May-22 :: Starts*/
IF UPPER(C.PRODUCT_TYPE) = UPPER('DVService') THEN

SELECT COUNT(1) 
        INTO V_PHONENUM_ATTR_CNT
        FROM NY_MIG_ORDER_ATTRIBUTES OA
        WHERE FILE_ID = V_FILE_ID 
        AND UPPER(ATTRIBUTENAME) = 'VEPC_ATTR_RT_PHONE_NUMBER_TYPE'
        AND UPPER(ATTRIBUTEVALUE) = 'PORTED'
        AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING DAM WHERE DAM.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(DAM.PRODUCT_TYPE) = UPPER('DVService'));
/* Added By Elen Product tree counts */
SELECT COUNT(1)
INTO V_PRODUCTTREE_CNT
FROM NY_MIG_PRODUCT_TREE P
WHERE FILE_ID = V_FILE_ID 
AND UPPER(P.PRODUCTNAME) = UPPER('Temporary NumberPort');

IF V_PHONENUM_ATTR_CNT > 0 AND V_PRODUCTTREE_CNT = 0 THEN

		PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,V_D_PROD_TYPE||' Number Portability product tree handling','PRC_SRC_PRODUCT_TREE_UPDATE','Loading the Number Portability into the product tree table','NEW', v_log_message, SYSDATE ); 

 INSERT INTO NY_MIG_PRODUCT_TREE (
				MAPPING_TYPE,
				PRODUCT_TYPE,
				PRODUCT_ID,
				S_CODE,
				NAME,
				LINK_TO_SERVICE_POINT,
				CONTRACT_TERM,
				PARENT_S_CODE,
				PARENT,
				RULES,
				PRODUCT_FAMILY,
                ATTRIBUTE_TYPE,
                ATTRIBUTE_DISPLAY_NAME,
                UNITPRICE,
                DVCPE_DEVICE_QUANTITY,
                DISCOUNTAMOUNT,
                PRODUCTTERM,
                PRODUCTTERMUNIT,
				FILE_ID,
				ORD_PROD_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				APPOINTMENT_REFERENCE__C,
				APPOINTMENTENDDATE,
				APPOINTMENTSTATUS,
				APPOINTMENTSTARTDATE,
				APPOINTMENTCREATEDDATE,
				APPOINTMENTPRODUCTTYPE,
				VLOCITY_CMT__ONETIMECHARGE__C,
				VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
				PRODUCTNAME,
				VLOCITY_CMT__LINENUMBER__C,
				VLOCITY_CMT__FULFILMENTSTATUS__C,
				VLOCITY_CMT__ISPONRREACHED__C,
				VLOCITY_CMT__PROVISIONINGSTATUS__C,
				PRODUCTCODE,
				PRODUCTID,
				NG_CUSTOMERREQUESTEDDATE__C,
				VLOCITY_CMT__RECURRINGCHARGE__C,
				QUANTITY,
				VLOCITY_CMT__RECURRINGTOTAL__C,
				ORDERITEMNUMBER,
				VLOCITY_CMT__ACTION__C,
                COM_CALC_RECURRINGCHARGE__C,
                DELIVERY_INSTRUCTIONS__C,
                APPOINTMENTENGINEER_INSTRUCTIONS__C,
                APPOINTMENTDELIVERY_PHONE__C,
				STATUS,
                VLOCITY_CMT__SUBACTION__C,
                OV_ACTION	,
                FULFILLMENTINSTRUCTION,                 /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                CEASE_DATE                              /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
				)


SELECT 
				AM.MAPPING_TYPE,
				AM.PRODUCT_TYPE,
				OP.PRODUCTCODE AS PRODUCT_ID,
				AM.S_CODE,
				AM.NAME,
				AM.LINK_TO_SERVICE_POINT,
				AM.CONTRACT_TERM,
				AM.PARENT_S_CODE,
				AM.PARENT,
				AM.RULES,
				AM.PRODUCT_FAMILY,
                AM.ATTRIBUTE_TYPE,
                AM.ATTRIBUTE_DISPLAY_NAME,
               (CASE WHEN INSTR ( ',' || AM.RULES || ','  , ',7,'  ) >0 THEN '[FM.recurringPrice]' 
                        WHEN INSTR (',' || AM.RULES || ','  , ',8,'  ) >0 THEN '[FM.onetimePrice]'
                        WHEN INSTR (',' || AM.RULES || ','  , ',13,'  ) >0 THEN '[FM.onetimePrice]' ELSE NULL END) AS unitPrice, /* SFI-31718 - BB Unit price issue - Added By Elen on 23/Jun/2022 */
                '' DVCPE_DEVICE_QUANTITY,
                '' DISCOUNTAMOUNT,
                '' PRODUCTTERM,
                '' PRODUCTTERMUNIT,
			OP.FILE_ID,
			OP.ORD_PROD_ID,
			OP.ORDERNUMBER,
			OP.BILLINGACCOUNTNO,
			OP.APPOINTMENT_REFERENCE__C,
			OP.APPOINTMENTENDDATE,
			OP.APPOINTMENTSTATUS,
			OP.APPOINTMENTSTARTDATE,
			OP.APPOINTMENTCREATEDDATE,
			OP.APPOINTMENTPRODUCTTYPE,
			OP.VLOCITY_CMT__ONETIMECHARGE__C,
			OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
			OP.PRODUCTNAME,
			OP.VLOCITY_CMT__LINENUMBER__C,
			OP.VLOCITY_CMT__FULFILMENTSTATUS__C,
			OP.VLOCITY_CMT__ISPONRREACHED__C,
			OP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
			OP.PRODUCTCODE,
			OP.PRODUCTID,
			OP.NG_CUSTOMERREQUESTEDDATE__C,
			OP.VLOCITY_CMT__RECURRINGCHARGE__C,
			OP.QUANTITY,
			OP.VLOCITY_CMT__RECURRINGTOTAL__C,
			OP.ORDERITEMNUMBER,
			OP.VLOCITY_CMT__ACTION__C,
            OP.COM_CALC_RECURRINGCHARGE__C,
            OP.DELIVERY_INSTRUCTIONS__C,
            OP.APPOINTMENTENGINEER_INSTRUCTIONS__C,
            OP.APPOINTMENTDELIVERY_PHONE__C,
			OP.STATUS,
            OP.VLOCITY_CMT__SUBACTION__C,
			 'Add' as OV_ACTION ,                     /* Added by bala for N 2 N regrade */
             OP.FULFILLMENTINSTRUCTION,               /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
             OP.CEASE_DATE                            /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
			FROM
				NY_MIG_ORDER_PRODUCT OP,
				DRIVER_ASSET_MAPPING AM,
                (SELECT * FROM NY_MIG_ORDER_ATTRIBUTES OA WHERE OA.FILE_ID = V_FILE_ID  AND UPPER(OA.ATTRIBUTENAME) = UPPER('VEPC_ATTR_RT_PHONE_NUMBER_TYPE')
                 AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('DVService'))) OA -- Updated by RUDHRA KUMAR on 18-MAY-22
			WHERE
			OP.ORDERNUMBER = OA.ORDERNUMBER
			AND OP.PRODUCTCODE = OA.PRODUCTCODE
			AND OP.FILE_ID = V_FILE_ID 
            AND UPPER(OP.VLOCITY_CMT__ACTION__C) <> UPPER('Disconnect')    -- Added for N2N regrade scenario
			AND UPPER(AM.MAPPING_TYPE) <> 'REDUNDANT'
			AND UPPER(AM.PRODUCT_ID) = UPPER('v_NumberPort');

			COMMIT;

	    END IF;


END IF;				

/*NUMBER port Scenarios part of C1 :: Added by kanagavel on 18-May-22 :: ends*/

/*ACCESS TECHNOLOGY BBU / ONT free product part of C1 :: Added by kanagavel on 05-May-22 :: Starts*/
IF Regexp_count(v_rules,'20') >0 THEN
SELECT COUNT(1) 
    INTO V_ORDER_ATTR_CNT
    FROM NY_MIG_ORDER_ATTRIBUTES OA
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(ATTRIBUTENAME) = 'VEPC_ATTR_RT_ACCESS_TECHNOLOGY'
    AND UPPER(ATTRIBUTEVALUE) IN ('SOGEA','FTTP')
    AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OA.PRODUCTCODE AND AM1.PRODUCT_TYPE = 'BB');	

	IF V_ORDER_ATTR_CNT = 0 THEN

	      DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND PRODUCT_TYPE = 'DVBBU';
          COMMIT;
    END IF;

SELECT PAR_NAME, PAR_VAL
    INTO V_NAME, V_PROD_CODE
    FROM NAYAN_CONFIG_PARAMS
    WHERE PAR_TYPE = 'SOGEA';

    --Check if we have attribute name VEPC_ATTR_RT_ACCESS_TECHNOLOGY with value as SOGEA against N0001032 (Broadband Services) in json input, then send the BBU charge product in xml.	
    SELECT COUNT(1) 
    INTO V_ORDER_ATTR_CNT
    FROM NY_MIG_ORDER_ATTRIBUTES OA
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(ATTRIBUTENAME) = 'VEPC_ATTR_RT_ACCESS_TECHNOLOGY'
    AND UPPER(ATTRIBUTEVALUE) = 'SOGEA'
    AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OA.PRODUCTCODE AND AM1.PRODUCT_TYPE = 'BB');	

    IF V_ORDER_ATTR_CNT = 1 THEN

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND S_CODE <> V_PROD_CODE AND PRODUCT_TYPE = V_NAME;
          COMMIT;
    END IF;
END IF;    

/*ACCESS TECHNOLOGY BBU charge product part of C1 :: Added by kanagavel on 05-May-22 :: Ends*/

/* DVCPE DUAL, TRIPLE, QUAD VALIDATION CHANGES Starts */
IF UPPER(C.PRODUCT_TYPE) = UPPER('DVCPEPack') THEN

        SELECT COUNT(1) 
        INTO V_ORDER_ATTR_CNT
        FROM NY_MIG_ORDER_ATTRIBUTES OA
        WHERE FILE_ID = V_FILE_ID 
        AND UPPER(ATTRIBUTENAME) LIKE 'VEPC_ATTR_DT_DEVICE_QUANTITY'
        AND ATTRIBUTEVALUE IS NOT NULL
        AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM WHERE AM.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM.PRODUCT_TYPE) = UPPER('DVCPEPack'));

        IF V_ORDER_ATTR_CNT = 1 THEN

            SELECT ATTRIBUTEVALUE 
            INTO v_quantity
            FROM NY_MIG_ORDER_ATTRIBUTES
            WHERE FILE_ID = V_FILE_ID 
            AND UPPER(ATTRIBUTENAME) LIKE 'VEPC_ATTR_DT_DEVICE_QUANTITY';

            BEGIN
                SELECT PAR_VAL
                INTO V_DVCPE_QUANTITY
                FROM NAYAN_CONFIG_PARAMS
                WHERE PAR_TYPE = 'VOICE'
                AND PAR_NAME = 'DVCPE_QUANTITY'
                AND PAR_VAL = v_quantity;

            Exception
            WHEN OTHERS THEN
            V_ERR_LOG_MSG := 'DVCPEpack device quantity must be in Single,dual,triple and quad value - VEPC_ATTR_DT_DEVICE_QUANTITY => '|| v_quantity || ' Parent_s_code => ' || C.PARENT_S_CODE;
            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_PRODUCT_TREE_UPDATE', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
            END;


        END IF;       

    SELECT COUNT(1)
    INTO V_PRODUCTTREE_CNT
    FROM NY_MIG_PRODUCT_TREE
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(PRODUCT_TYPE) = UPPER('DVCPE');

	IF V_PRODUCTTREE_CNT = 0 AND V_ORDER_ATTR_CNT > 0 AND V_DVCPE_QUANTITY IS NOT NULL THEN
		PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'DVCPEPack product tree handling','PRC_SRC_PRODUCT_TREE_UPDATE','Loading the DV CPEPack into the product tree table','NEW', v_log_message, SYSDATE );

            INSERT INTO NY_MIG_PRODUCT_TREE (
				MAPPING_TYPE,
				PRODUCT_TYPE,
				PRODUCT_ID,
				S_CODE,
				NAME,
				LINK_TO_SERVICE_POINT,
				CONTRACT_TERM,
				PARENT_S_CODE,
				PARENT,
				RULES,
				PRODUCT_FAMILY,
                ATTRIBUTE_TYPE,
                ATTRIBUTE_DISPLAY_NAME,
                UNITPRICE,
                DVCPE_DEVICE_QUANTITY,
                DISCOUNTAMOUNT,
                PRODUCTTERM,
                PRODUCTTERMUNIT,
				FILE_ID,
				ORD_PROD_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				APPOINTMENT_REFERENCE__C,
				APPOINTMENTENDDATE,
				APPOINTMENTSTATUS,
				APPOINTMENTSTARTDATE,
				APPOINTMENTCREATEDDATE,
				APPOINTMENTPRODUCTTYPE,
				VLOCITY_CMT__ONETIMECHARGE__C,
				VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
				PRODUCTNAME,
				VLOCITY_CMT__LINENUMBER__C,
				VLOCITY_CMT__FULFILMENTSTATUS__C,
				VLOCITY_CMT__ISPONRREACHED__C,
				VLOCITY_CMT__PROVISIONINGSTATUS__C,
				PRODUCTCODE,
				PRODUCTID,
				NG_CUSTOMERREQUESTEDDATE__C,
				VLOCITY_CMT__RECURRINGCHARGE__C,
				QUANTITY,
				VLOCITY_CMT__RECURRINGTOTAL__C,
				ORDERITEMNUMBER,
				VLOCITY_CMT__ACTION__C,
                COM_CALC_RECURRINGCHARGE__C,
                DELIVERY_INSTRUCTIONS__C,
                APPOINTMENTENGINEER_INSTRUCTIONS__C,
                APPOINTMENTDELIVERY_PHONE__C,
				STATUS,
                VLOCITY_CMT__SUBACTION__C,
                OV_ACTION  ,                    /* Added by bala for N 2 N regrade */
				FULFILLMENTINSTRUCTION,         /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                CEASE_DATE                      /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                )
SELECT 
				AM.MAPPING_TYPE,
				AM.PRODUCT_TYPE,
				OP.PRODUCTCODE AS PRODUCT_ID,
				AM.S_CODE,
				AM.NAME,
				AM.LINK_TO_SERVICE_POINT,
				AM.CONTRACT_TERM,
				AM.PARENT_S_CODE,
				AM.PARENT,
				AM.RULES,
				AM.PRODUCT_FAMILY,
                AM.ATTRIBUTE_TYPE,
                AM.ATTRIBUTE_DISPLAY_NAME,
                (CASE WHEN INSTR ( ',' || AM.RULES || ','  , ',7,'  ) >0 THEN '[FM.recurringPrice]' 
                        WHEN INSTR (',' || AM.RULES || ','  , ',8,'  ) >0 THEN '[FM.onetimePrice]'
                        WHEN INSTR (',' || AM.RULES || ','  , ',13,'  ) >0 THEN '[FM.onetimePrice]' ELSE NULL END) AS unitPrice, /* SFI-31718 - BB Unit price issue - Added By Elen on 23/Jun/2022 */
                v_quantity AS DVCPE_DEVICE_QUANTITY,
                '' DISCOUNTAMOUNT,
                '' PRODUCTTERM,
                '' PRODUCTTERMUNIT,
			OP.FILE_ID,
			OP.ORD_PROD_ID,
			OP.ORDERNUMBER,
			OP.BILLINGACCOUNTNO,
			OP.APPOINTMENT_REFERENCE__C,
			OP.APPOINTMENTENDDATE,
			OP.APPOINTMENTSTATUS,
			OP.APPOINTMENTSTARTDATE,
			OP.APPOINTMENTCREATEDDATE,
			OP.APPOINTMENTPRODUCTTYPE,
			OP.VLOCITY_CMT__ONETIMECHARGE__C,
			OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
			OP.PRODUCTNAME,
			OP.VLOCITY_CMT__LINENUMBER__C,
			OP.VLOCITY_CMT__FULFILMENTSTATUS__C,
			OP.VLOCITY_CMT__ISPONRREACHED__C,
			OP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
			OP.PRODUCTCODE,
			OP.PRODUCTID,
			OP.NG_CUSTOMERREQUESTEDDATE__C,
			OP.VLOCITY_CMT__RECURRINGCHARGE__C,
			OP.QUANTITY,
			OP.VLOCITY_CMT__RECURRINGTOTAL__C,
			OP.ORDERITEMNUMBER,
			OP.VLOCITY_CMT__ACTION__C,
            OP.COM_CALC_RECURRINGCHARGE__C,
            OP.DELIVERY_INSTRUCTIONS__C,
            OP.APPOINTMENTENGINEER_INSTRUCTIONS__C,
            OP.APPOINTMENTDELIVERY_PHONE__C,
			OP.STATUS,
            OP.VLOCITY_CMT__SUBACTION__C,
			 'Add' as OV_ACTION ,                     /* Added by bala for N 2 N regrade */
			OP.FULFILLMENTINSTRUCTION,                /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
            OP.CEASE_DATE                             /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
            FROM
				NY_MIG_ORDER_PRODUCT OP,
				(SELECT * FROM DRIVER_ASSET_MAPPING WHERE UPPER(MAPPING_TYPE) <> 'REDUNDANT' AND UPPER(PRODUCT_ID) = UPPER('v_DVCPE') AND UPPER(PRODUCT_TYPE) =  UPPER('DVCPE')) AM,
                (SELECT * FROM NY_MIG_ORDER_ATTRIBUTES OA WHERE OA.FILE_ID = V_FILE_ID  AND UPPER(OA.ATTRIBUTENAME) = UPPER('VEPC_ATTR_DT_DEVICE_NAME')
                 AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('DVCPEPack'))) OA
			WHERE
			OP.ORDERNUMBER = OA.ORDERNUMBER
            AND UPPER(OP.VLOCITY_CMT__ACTION__C) <> UPPER('Disconnect')    -- Added for N2N regrade scenario
			AND OP.PRODUCTCODE = OA.PRODUCTCODE
			AND OP.FILE_ID = V_FILE_ID 
            AND (
/*
                CASE
                WHEN OA.PRODUCTCODE IN ( 'N0001007', 'N0001142', 'N0001065', 'N0001005' ) THEN 'S0359821'
                WHEN OA.PRODUCTCODE IN ( 'N0001067', 'N0001008', 'N0001064', 'N0001143' ) THEN 'S0505504'
                WHEN OA.PRODUCTCODE IN ( 'N0001066', 'N0001006', 'N0001141', 'N0001063' ) THEN 'S0505505'
                END
*/
                CASE
                WHEN OA.PRODUCTCODE IN ( 'N0001142','N0001007','N0001065','N0001005' ) THEN 'S0571002'
                WHEN OA.PRODUCTCODE IN ( 'N0001067','N0001008','N0001064','N0001143' ) THEN 'S0570986'
                WHEN OA.PRODUCTCODE IN ( 'N0001066','N0001006','N0001141','N0001063' ) THEN 'S0570987'
                END
            ) = AM.S_CODE;

    END IF;

END IF; 
/* DVCPE DUAL, TRIPLE, QUAD VALIDATION CHANGES Ends */


/*Added By Siva - BB - For Smart Hub C1 Changes > Start*/

IF UPPER(C.PRODUCT_TYPE) = 'BBCPE'  THEN

    /* Only one BBCPE is possible for a customer, to create the S code with below logic,
       S0387454 - If the existing order holds this S codes then the same should be sent in the order. */

	SELECT PAR_NAME, PAR_VAL
    INTO V_NAME, V_PROD_CODE
    FROM NAYAN_CONFIG_PARAMS
    WHERE PAR_TYPE = '30'  
     AND PAR_NAME = 'BBCPE';   


    SELECT COUNT(1) 
    INTO V_ORDER_ATTR_CNT
	FROM  NY_MIG_TMF_ORDERPRODUCT NMTOP
    WHERE NMTOP.PRODUCTSPECIFICATIONID  = V_PROD_CODE AND NMTOP.ORDERNUMBER = v_ordernumber ;


	IF V_ORDER_ATTR_CNT = 1 THEN	

      DELETE 
	   FROM NY_MIG_PRODUCT_TREE
	  WHERE FILE_ID = V_FILE_ID 
  	    AND S_CODE <> V_PROD_CODE
		AND UPPER(PRODUCT_TYPE) = UPPER('BBCPE'); 

	COMMIT;

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule 30>Deleting other scodes','PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

     ELSE 	

      DELETE 
	   FROM NY_MIG_PRODUCT_TREE
	  WHERE FILE_ID = V_FILE_ID 
  	    AND S_CODE = V_PROD_CODE
		AND UPPER(PRODUCT_TYPE) = UPPER('BBCPE'); 

	COMMIT;

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule 30>Deleting S_CODE=>'||V_PROD_CODE,'PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );	 

	END IF;


END IF; 

IF Regexp_count(v_rules,'24') >0 THEN


	SELECT PAR_NAME, PAR_VAL
    INTO V_NAME, V_PROD_CODE
    FROM NAYAN_CONFIG_PARAMS
    WHERE PAR_TYPE = '24'
     AND PAR_NAME = 'BBCPE'
     AND PAR_VAL = C.S_CODE; 

    /* Check for attribute VEPC_ATTR_ACCESS_TECHNOLOGY = FTTP against N0001001 in json input, then create the S0565266 product in xml. */
    SELECT COUNT(1) 
    INTO V_ORDER_ATTR_CNT
    FROM NY_MIG_ORDER_ATTRIBUTES OA
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(ATTRIBUTENAME) = 'VEPC_ATTR_ACCESS_TECHNOLOGY'
    AND UPPER(ATTRIBUTEVALUE) IN ('FTTP')
    AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID =  OA.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('BBCPE') 
					) ;


        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.24>V_ORDER_ATTR_CNT=>'||V_ORDER_ATTR_CNT,'PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

    IF V_ORDER_ATTR_CNT = 0 THEN

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.24.Deleting data.Rule not passed','PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND UPPER(PRODUCT_TYPE) = UPPER('BBCPE') 
			                  AND UPPER(S_CODE) = V_PROD_CODE;
            COMMIT;

     ELSE

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.24.Deleting data..Rule passed','PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND UPPER(PRODUCT_TYPE) = UPPER('BBCPE') 
			                  AND UPPER(S_CODE) NOT IN ( V_PROD_CODE,'S0387454');
            COMMIT;	 
    END IF;

END IF;

/* Manage NOW TV Addon Added by Raju Palle On 09-08-22      --START*/


IF Regexp_count (v_rules,'36') > 0  THEN

 SELECT (CASE WHEN FULFILLMENTINSTRUCTION = 'Add' THEN 1
                WHEN FULFILLMENTINSTRUCTION = 'AutoRenewOff' THEN 2
                WHEN FULFILLMENTINSTRUCTION = 'AutoRenewOn'  THEN 3
                WHEN FULFILLMENTINSTRUCTION = 'Retain' THEN 4       
                WHEN FULFILLMENTINSTRUCTION = 'Replace' THEN 5
			    ELSE 0 END) 
                INTO V_PRODUCTTREE_CNT
                FROM NY_MIG_PRODUCT_TREE
                WHERE FILE_ID = V_FILE_ID 
                AND UPPER(PRODUCT_TYPE) = UPPER('NTVAddOn');



IF V_PRODUCTTREE_CNT > 1 THEN 
PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'NTV AddOn product tree handling','PRC_SRC_PRODUCT_TREE_UPDATE','Loading the Dynamic s_code into the product tree table','NEW', v_log_message, SYSDATE );

            INSERT INTO NY_MIG_PRODUCT_TREE (
				MAPPING_TYPE,
				PRODUCT_TYPE,
				PRODUCT_ID,
				S_CODE,
				NAME,
				LINK_TO_SERVICE_POINT,
				CONTRACT_TERM,
				PARENT_S_CODE,
				PARENT,
				RULES,
				PRODUCT_FAMILY,
                ATTRIBUTE_TYPE,
                ATTRIBUTE_DISPLAY_NAME,
                UNITPRICE,
                DVCPE_DEVICE_QUANTITY,
                DISCOUNTAMOUNT,
                PRODUCTTERM,
                PRODUCTTERMUNIT,
				FILE_ID,
				ORD_PROD_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				APPOINTMENT_REFERENCE__C,
				APPOINTMENTENDDATE,
				APPOINTMENTSTATUS,
				APPOINTMENTSTARTDATE,
				APPOINTMENTCREATEDDATE,
				APPOINTMENTPRODUCTTYPE,
				VLOCITY_CMT__ONETIMECHARGE__C,
				VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
				PRODUCTNAME,
				VLOCITY_CMT__LINENUMBER__C,
				VLOCITY_CMT__FULFILMENTSTATUS__C,
				VLOCITY_CMT__ISPONRREACHED__C,
				VLOCITY_CMT__PROVISIONINGSTATUS__C,
				PRODUCTCODE,
				PRODUCTID,
				NG_CUSTOMERREQUESTEDDATE__C,
				VLOCITY_CMT__RECURRINGCHARGE__C,
				QUANTITY,
				VLOCITY_CMT__RECURRINGTOTAL__C,
				ORDERITEMNUMBER,
				VLOCITY_CMT__ACTION__C,
                COM_CALC_RECURRINGCHARGE__C,
                DELIVERY_INSTRUCTIONS__C,
                APPOINTMENTENGINEER_INSTRUCTIONS__C,
                APPOINTMENTDELIVERY_PHONE__C,
				STATUS ,
                VLOCITY_CMT__SUBACTION__C,
                OV_ACTION  ,                    /* Added by bala for N 2 N regrade */	
                FULFILLMENTINSTRUCTION,         /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
                CEASE_DATE                      /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
				)

SELECT 

				AM.MAPPING_TYPE,
				AM.PRODUCT_TYPE,
				AM.PRODUCT_ID,
				AM.S_CODE,
				AM.NAME,
				AM.LINK_TO_SERVICE_POINT,
				AM.CONTRACT_TERM,
				AM.PARENT_S_CODE,
				AM.PARENT,
				AM.RULES,
				AM.PRODUCT_FAMILY,
                AM.ATTRIBUTE_TYPE,
                AM.ATTRIBUTE_DISPLAY_NAME,
                (CASE WHEN INSTR ( ',' || AM.RULES || ','  , ',7,'  ) >0 THEN '[FM.recurringPrice]' 
                        WHEN INSTR (',' || AM.RULES || ','  , ',8,'  ) >0 THEN '[FM.onetimePrice]'
                        WHEN INSTR (',' || AM.RULES || ','  , ',13,'  ) >0 THEN '[FM.onetimePrice]' ELSE NULL END) AS unitPrice, /* SFI-31718 - BB Unit price issue - Added By Elen on 23/Jun/2022 */
                '' DVCPE_DEVICE_QUANTITY,
                '' DISCOUNTAMOUNT,
                '' PRODUCTTERM,
                '' PRODUCTTERMUNIT,
			OP.FILE_ID,
			OP.ORD_PROD_ID,
			OP.ORDERNUMBER,
			OP.BILLINGACCOUNTNO,
			OP.APPOINTMENT_REFERENCE__C,
			OP.APPOINTMENTENDDATE,
			OP.APPOINTMENTSTATUS,
			OP.APPOINTMENTSTARTDATE,
			OP.APPOINTMENTCREATEDDATE,
			OP.APPOINTMENTPRODUCTTYPE,
			OP.VLOCITY_CMT__ONETIMECHARGE__C,
			OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
			AM.PRODUCT_NAME,
			OP.VLOCITY_CMT__LINENUMBER__C,
			OP.VLOCITY_CMT__FULFILMENTSTATUS__C,
			OP.VLOCITY_CMT__ISPONRREACHED__C,
			OP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
			OP.PRODUCTCODE,
			OP.PRODUCTID,
			OP.NG_CUSTOMERREQUESTEDDATE__C,
			OP.VLOCITY_CMT__RECURRINGCHARGE__C,
			OP.QUANTITY,
			OP.VLOCITY_CMT__RECURRINGTOTAL__C,
			OP.ORDERITEMNUMBER,
			OP.VLOCITY_CMT__ACTION__C,
            OP.COM_CALC_RECURRINGCHARGE__C,
            OP.DELIVERY_INSTRUCTIONS__C,
            OP.APPOINTMENTENGINEER_INSTRUCTIONS__C,
            OP.APPOINTMENTDELIVERY_PHONE__C,
			OP.STATUS,
            OP.VLOCITY_CMT__SUBACTION__C,
			 'Add' as OV_ACTION ,                     /* Added by bala for N 2 N regrade */
             OP.FULFILLMENTINSTRUCTION,               /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
             OP.CEASE_DATE                            /* CHANGED BY RUDHRA KUMAR ON 22-JULY-2022 FOR BRITBOX */
			FROM
				NY_MIG_ORDER_PRODUCT OP,
				DRIVER_ASSET_MAPPING AM
			WHERE OP.FILE_ID = V_FILE_ID 
            AND UPPER(OP.VLOCITY_CMT__ACTION__C) <> UPPER('Disconnect')    -- Added for N2N regrade scenario
			AND UPPER(AM.MAPPING_TYPE) <> 'REDUNDANT'
			AND UPPER(AM.PRODUCT_ID) = UPPER('v_NTVAddOn')
            AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID = OP.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('NTVAddOn'));
			COMMIT;

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND UPPER(PRODUCT_TYPE) = UPPER('NTVAddOn');
            COMMIT;

	 ELSIF 	V_PRODUCTTREE_CNT = 1 THEN 

		DELETE FROM NY_MIG_PRODUCT_TREE 
				WHERE FILE_ID = V_FILE_ID 
				AND S_CODE ='S0392437'
				AND PRODUCT_ID = 'v_NTVAddOn';

				COMMIT;

      ELSIF               
                V_PRODUCTTREE_CNT = 0 THEN 
				DELETE FROM NY_MIG_PRODUCT_TREE 
				WHERE FILE_ID = V_FILE_ID 
				--AND S_CODE ='S0392437'
                AND UPPER(PRODUCT_TYPE) = UPPER('NTVAddOn');

                COMMIT;

                END IF;

		END IF;

/* Manage NOW TV Addon Added by Raju Palle On 09-08-22      --END*/

IF Regexp_count(v_rules,'25') >0 THEN

    /* To create S0565264,
   i) Check for attribute VEPC_ATTR_ACCESS_TECHNOLOGY = SOGEA against N0001001.
   ii) Appointment link should not be available against N0001001.  
   iii) Should not be a regrade order.	
   */ 

	SELECT PAR_NAME, PAR_VAL
    INTO V_NAME, V_PROD_CODE
    FROM NAYAN_CONFIG_PARAMS
    WHERE PAR_TYPE = '25'
     AND PAR_NAME = 'BBCPE'
     AND PAR_VAL = C.S_CODE; 

    SELECT COUNT(*) 
    INTO V_ORDER_ATTR_CNT
    FROM NY_MIG_ORDER_ATTRIBUTES MOT
    WHERE ORDERNUMBER = v_ordernumber
      AND UPPER(TRIM(ATTRIBUTENAME))=  'VEPC_ATTR_ACCESS_TECHNOLOGY'   --Needs to be change based on attribute name
	  AND UPPER(ATTRIBUTEVALUE)= UPPER('SOGEA')
	  AND NOT EXISTS ( SELECT 1 FROM  NY_MIG_ORDER_PRODUCT MOP WHERE MOT.ORDERNUMBER= MOP.ORDERNUMBER AND MOP.PRODUCTNAME IN (  'Broadband Services', 'Managed Install','Self Install')
	                 AND APPOINTMENT_REFERENCE__C IS NOT NULL  )
      AND EXISTS ( SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID =  MOT.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('BBCPE')
				   )
	  AND NVL(V_IS_REGRADE,'N') = 'N' 
    ;

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.25>V_ORDER_ATTR_CNT=>'||V_ORDER_ATTR_CNT,'PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

    IF V_ORDER_ATTR_CNT = 0 THEN

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.25.Deleting data..Rule not passed','PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE ORDERNUMBER = v_ordernumber AND UPPER(PRODUCT_TYPE) = UPPER('BBCPE')
         			AND UPPER(S_CODE) = V_PROD_CODE ;
            COMMIT;
     ELSE

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.25.Deleting data..Rule passed','PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE ORDERNUMBER = v_ordernumber AND UPPER(PRODUCT_TYPE) = UPPER('BBCPE') 
			                  AND UPPER(S_CODE) NOT IN ( V_PROD_CODE,'S0387454');
            COMMIT;			

    END IF;

END IF;

IF Regexp_count(v_rules,'26') >0 THEN

    /* To create S0565265,
   i) Check for attribute VEPC_ATTR_ACCESS_TECHNOLOGY = SOGEA against N0001001.
   ii) Appointment link should be available against N0001001.
   iii) Should not be a regrade order.	*/

	SELECT PAR_NAME, PAR_VAL
    INTO V_NAME, V_PROD_CODE
    FROM NAYAN_CONFIG_PARAMS
    WHERE PAR_TYPE = '26'
     AND PAR_NAME = 'BBCPE'
     AND PAR_VAL = C.S_CODE; 

    SELECT COUNT(*) 
    INTO V_ORDER_ATTR_CNT
    FROM NY_MIG_ORDER_ATTRIBUTES MOT 
    WHERE FILE_ID = V_FILE_ID 
      AND UPPER(TRIM(ATTRIBUTENAME))=  'VEPC_ATTR_ACCESS_TECHNOLOGY'   --Needs to be change based on attribute name
	  AND UPPER(ATTRIBUTEVALUE)= UPPER('SOGEA')
	  AND EXISTS ( SELECT 1 FROM  NY_MIG_ORDER_PRODUCT MOP WHERE MOT.ORDERNUMBER= MOP.ORDERNUMBER AND MOP.PRODUCTNAME IN (  'Broadband Services', 'Managed Install','Self Install')
	                 AND APPOINTMENT_REFERENCE__C IS NOT NULL  )
      AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID =  MOT.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('BBCPE')  
				   )
	  AND NVL(V_IS_REGRADE,'N') = 'N'				   		   
      ;

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.26>V_ORDER_ATTR_CNT=>'||V_ORDER_ATTR_CNT,'PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

    IF V_ORDER_ATTR_CNT = 0 THEN

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.26.Deleting data..Rule not passed','PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID AND UPPER(PRODUCT_TYPE) = UPPER('BBCPE') 
			       AND UPPER(S_CODE) = V_PROD_CODE;
            COMMIT;
     ELSE

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.26.Deleting data..Rule passed','PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND UPPER(PRODUCT_TYPE) = UPPER('BBCPE') 
			                  AND UPPER(S_CODE) NOT IN ( V_PROD_CODE,'S0387454');
            COMMIT;				

    END IF;

END IF;

IF Regexp_count(v_rules,'27') >0 THEN

    /* To create S0565270,
   i) Check for attribute VEPC_ATTR_ACCESS_TECHNOLOGY = SOGEA against N0001001.
   ii) Should be a regrade order.*/

	SELECT PAR_NAME, PAR_VAL
    INTO V_NAME, V_PROD_CODE
    FROM NAYAN_CONFIG_PARAMS
    WHERE PAR_TYPE = '27'
     AND PAR_NAME = 'BBCPE'
     AND PAR_VAL = C.S_CODE;   

    SELECT COUNT(*) 
    INTO V_ORDER_ATTR_CNT
    FROM NY_MIG_ORDER_ATTRIBUTES MOT
    WHERE FILE_ID = V_FILE_ID 
      AND UPPER(TRIM(ATTRIBUTENAME))=  'VEPC_ATTR_ACCESS_TECHNOLOGY'   --Needs to be change based on attribute name
	  AND UPPER(ATTRIBUTEVALUE)= UPPER('SOGEA') 
      AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID =  MOT.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('BBCPE') 
				  )
	  AND NVL(V_IS_REGRADE,'N') = 'Y'			   
       ;

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.27>V_ORDER_ATTR_CNT=>'||V_ORDER_ATTR_CNT,'PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

    IF V_ORDER_ATTR_CNT = 0 THEN

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.27.Deleting data..Rule not passed','PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND UPPER(PRODUCT_TYPE) = UPPER('BBCPE') 
			   AND UPPER(S_CODE) = V_PROD_CODE;
            COMMIT;
     ELSE

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.27.Deleting data..Rule passed','PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND UPPER(PRODUCT_TYPE) = UPPER('BBCPE') 
			                  AND UPPER(S_CODE) NOT IN ( V_PROD_CODE,'S0387454');
            COMMIT;		
    END IF;
END IF;

/*Added By Siva - BB - For Smart Hub C1 Changes > End*/



/*Added By Siva - BBCHOP > Start*/

IF ( UPPER(C.PRODUCT_TYPE) IN ( 'BBCHOPCPE','BBSWCPE') AND Regexp_count(v_rules,'28') > 0 ) THEN

    /* Customer can have only any of 1 disc, any of 1 chop with him. If customer holds a disc [BBCWCPE], hybrid [BBCHOPCPE] product with him then the same S code has to be sent to OneView, if not then S0565268 for disc and S0565269 for hybrid has to be sent to OneView considering he is a new customer.*/

    SELECT COUNT(1) 
    INTO V_ORDER_ATTR_CNT
	FROM  NY_MIG_TMF_ORDERPRODUCT NMTOP
    WHERE NMTOP.PRODUCTSPECIFICATIONID  = C.S_CODE 
	AND NMTOP.FILE_ID = V_FILE_ID  ;


    IF V_ORDER_ATTR_CNT = 1 THEN

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule 28 Deleting other s_codes='||C.S_CODE,'PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

      DELETE 
	   FROM NY_MIG_PRODUCT_TREE
	  WHERE FILE_ID = V_FILE_ID 
  	    AND S_CODE <> C.S_CODE
		AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE); 

	COMMIT;

	ELSE 
	   IF C.S_CODE NOT IN ( 'S0565268','S0565269') THEN 

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule 28 Deleting s_codes='||C.S_CODE,'PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

		  DELETE 
		   FROM NY_MIG_PRODUCT_TREE
		  WHERE FILE_ID = V_FILE_ID 
			AND S_CODE = C.S_CODE
			AND UPPER(PRODUCT_TYPE) = UPPER(C.PRODUCT_TYPE); 

		COMMIT;	   

	   END IF;

    END IF;
END IF;


IF Regexp_count(v_rules,'29') > 0 THEN

    /* Check for attribute OrderProduct.vlocity_cmt__Action__c = Add against N0001032 in json input, then create the S0524164 product in xml.*/

    SELECT COUNT(*) 
    INTO V_ORDER_ATTR_CNT
    FROM NY_MIG_PRODUCT_TREE MOT 
    WHERE FILE_ID = V_FILE_ID 
      AND UPPER(VLOCITY_CMT__ACTION__C) =  'ADD' 
      AND UPPER(PRODUCT_TYPE) = UPPER('BB')  ;

    IF V_ORDER_ATTR_CNT = 0 THEN

        PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Rule.29.Deleting data..Rule not passed','PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );

            DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  
			   AND UPPER(S_CODE) = UPPER(C.S_CODE);
            COMMIT;	
    END IF;
END IF;
/*Added By Siva - BBCHOP > End*/

-- Start: Added by Siva for Carelevel Product

IF UPPER(C.PRODUCT_TYPE) = 'CARELEVEL' AND REGEXP_COUNT(v_rules,'32') >0 THEN 

   /* Use attribute "VEPC_ATTR_RT_CARE_LEVEL" under N0001032 (Broadband Services)  to add the Care level product. If it is sent as
        1> 'Standard', then we need to add 'S0131476' - 'Standard Care'.
		2> 'Priority fault repair' then we need to add 'S0132960' / 'Priority Fault Repair' */

    SELECT  COUNT(CASE WHEN UPPER(ATTRIBUTEVALUE) = 'STANDARD' THEN 'STANDARD' ELSE NULL END)  STD_COUNT_1,
            COUNT(CASE WHEN UPPER(ATTRIBUTEVALUE) = 'PRIORITY FAULT REPAIR' THEN 'PRIORITY FAULT REPAIR' ELSE NULL END)  STD_COUNT_2	
    INTO V_ORDER_ATTR_CNT,V_ORDER_ATTR_CNT_2
    FROM NY_MIG_ORDER_ATTRIBUTES OA
    WHERE FILE_ID = V_FILE_ID 
    AND UPPER(ATTRIBUTENAME) = 'VEPC_ATTR_RT_CARE_LEVEL' 
    AND EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM1 WHERE AM1.PRODUCT_ID =  OA.PRODUCTCODE AND UPPER(AM1.PRODUCT_TYPE) = UPPER('BB') 
					) ;


    PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Care Level Product Update > V_ORDER_ATTR_CNT > '||V_ORDER_ATTR_CNT,'PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );
    PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Care Level Product Update > V_ORDER_ATTR_CNT_2 > '||V_ORDER_ATTR_CNT_2,'PRC_SRC_PRODUCT_TREE_UPDATE',v_name,'NEW', v_log_message, SYSDATE );


     IF V_ORDER_ATTR_CNT = 1 THEN

          DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND UPPER(PRODUCT_TYPE) = 'CARELEVEL' AND S_CODE = 'S0132960' ;
          COMMIT; 

  	  ELSIF  V_ORDER_ATTR_CNT_2 = 1 THEN
           DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID  AND UPPER(PRODUCT_TYPE) = 'CARELEVEL' AND S_CODE = 'S0131476' ;
           COMMIT;   
     ELSE 
           DELETE FROM NY_MIG_PRODUCT_TREE WHERE FILE_ID = V_FILE_ID   AND UPPER(PRODUCT_TYPE) = 'CARELEVEL'   ;
           COMMIT;   		
     END IF ;

END IF;
-- End: Added by Siva for Carelevel Product	

END LOOP;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Product tree update','PRC_SRC_PRODUCT_TREE_UPDATE','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_PRODUCT_TREE_UPDATE', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_PRODUCT_TREE_UPDATE;

-- Regrade changes. Added parameter V_IS_REGRADE
PROCEDURE PRC_SRC_PRODUCT_TYPE_LINK(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER, V_IS_REGRADE IN VARCHAR2,V_REGRADE_TYPE IN VARCHAR2) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the Link product details using NY_MIG_PRODUCT_TREE','PRC_SRC_PRODUCT_TYPE_LINK','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_PRODUCT_LINK
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_PRODUCT_LINK (
PROD_TREE_ROWID,            
MAPPING_TYPE,
PRODUCT_TYPE,
PRODUCT_ID,
S_CODE,
NAME,
LINK_TO_SERVICE_POINT,
CONTRACT_TERM,
PARENT_S_CODE,
PARENT,
RULES,
PRODUCT_FAMILY,
ATTRIBUTE_TYPE,
ATTRIBUTE_DISPLAY_NAME,
UNITPRICE,
FILE_ID,
ORD_PROD_ID,
ORDERNUMBER,
BILLINGACCOUNTNO,
APPOINTMENT_REFERENCE__C,
APPOINTMENTENDDATE,
APPOINTMENTSTATUS,
APPOINTMENTSTARTDATE,
APPOINTMENTCREATEDDATE,
APPOINTMENTPRODUCTTYPE,
VLOCITY_CMT__ONETIMECHARGE__C,
VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
PRODUCTNAME,
VLOCITY_CMT__LINENUMBER__C,
VLOCITY_CMT__FULFILMENTSTATUS__C,
VLOCITY_CMT__ISPONRREACHED__C,
VLOCITY_CMT__PROVISIONINGSTATUS__C,
PRODUCTCODE,
PRODUCTID,
NG_CUSTOMERREQUESTEDDATE__C,
VLOCITY_CMT__RECURRINGCHARGE__C,
QUANTITY,
VLOCITY_CMT__RECURRINGTOTAL__C,
ORDERITEMNUMBER,
VLOCITY_CMT__ACTION__C,
VLOCITY_CMT__SUBACTION__C,
COM_CALC_RECURRINGCHARGE__C,
DELIVERY_INSTRUCTIONS__C,
STATUS,
LINK_NAME,
LINK_PRODUCT,
LINK_PRODUCT_SCODE,
LINK_PRODUCT_NAME,
LINK_PRODUCT_SCODE_VALUE
)
SELECT
    C_NP.ROWID AS PROD_TREE_ROWID,
    C_NP.MAPPING_TYPE,
    C_NP.PRODUCT_TYPE,
    C_NP.PRODUCT_ID,
    C_NP.S_CODE,
    C_NP.NAME,
    C_NP.LINK_TO_SERVICE_POINT,
    C_NP.CONTRACT_TERM,
    C_NP.PARENT_S_CODE,
    C_NP.PARENT,
    C_NP.RULES,
    C_NP.PRODUCT_FAMILY,
    C_NP.ATTRIBUTE_TYPE,
    C_NP.ATTRIBUTE_DISPLAY_NAME,
    C_NP.UNITPRICE,
    C_NP.FILE_ID,
    C_NP.ORD_PROD_ID,
    C_NP.ORDERNUMBER,
    C_NP.BILLINGACCOUNTNO,
    C_NP.APPOINTMENT_REFERENCE__C,
    C_NP.APPOINTMENTENDDATE,
    C_NP.APPOINTMENTSTATUS,
    C_NP.APPOINTMENTSTARTDATE,
    C_NP.APPOINTMENTCREATEDDATE,
    C_NP.APPOINTMENTPRODUCTTYPE,
    C_NP.VLOCITY_CMT__ONETIMECHARGE__C,
    C_NP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,
    C_NP.PRODUCTNAME,
    C_NP.VLOCITY_CMT__LINENUMBER__C,
    C_NP.VLOCITY_CMT__FULFILMENTSTATUS__C,
    C_NP.VLOCITY_CMT__ISPONRREACHED__C,
    C_NP.VLOCITY_CMT__PROVISIONINGSTATUS__C,
    C_NP.PRODUCTCODE,
    C_NP.PRODUCTID,
    C_NP.NG_CUSTOMERREQUESTEDDATE__C,
    C_NP.VLOCITY_CMT__RECURRINGCHARGE__C,
    C_NP.QUANTITY,
    C_NP.VLOCITY_CMT__RECURRINGTOTAL__C,
    C_NP.ORDERITEMNUMBER,
    C_NP.VLOCITY_CMT__ACTION__C,
    C_NP.VLOCITY_CMT__SUBACTION__C,
    C_NP.COM_CALC_RECURRINGCHARGE__C,
    C_NP.DELIVERY_INSTRUCTIONS__C,
    C_NP.STATUS,
    PL.LINK_NAME AS LINK_NAME,
    PL.LINK_PRODUCT,
    P_NP.S_CODE  AS LINK_PRODUCT_SCODE,
    P_NP.NAME    AS LINK_PRODUCT_NAME,
    (  CASE  WHEN P_NP.S_CODE IS NOT NULL THEN  'DUMMY_' || P_NP.S_CODE  END  ) AS LINK_PRODUCT_SCODE_VALUE
  FROM
    NY_MIG_PRODUCT_TREE C_NP,
    DRIVER_PRODUCT_LINK PL,
    NY_MIG_PRODUCT_TREE P_NP
 WHERE
    C_NP.FILE_ID = V_FILE_ID 
       AND UPPER( C_NP.PRODUCT_TYPE ) = UPPER( PL.PRODUCT_TYPE(+) )
       AND UPPER( PL.LINK_PRODUCT  )   = UPPER( P_NP.PRODUCT_TYPE(+) )
       AND C_NP.ORDERNUMBER         = P_NP.ORDERNUMBER (+)
 ORDER BY
    C_NP.PARENT,
    C_NP.MAPPING_TYPE DESC,
    C_NP.ORD_PROD_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the Link product details using NY_MIG_PRODUCT_TREE','PRC_SRC_PRODUCT_TYPE_LINK','Link product mapping record loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the Link product details using NY_MIG_PRODUCT_TREE','PRC_SRC_PRODUCT_TYPE_LINK','Link product mapping already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the Link product details using NY_MIG_PRODUCT_TREE','PRC_SRC_PRODUCT_TYPE_LINK','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_PRODUCT_TYPE_LINK', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_PRODUCT_TYPE_LINK;


PROCEDURE PRC_SRC_TOD_WORKING_LINES(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD Working LINES from JSON Source file','PRC_SRC_TOD_WORKING_LINES','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_WORKING_LINES
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_WORKING_LINES 
(
			FILE_ID,
			ORDERNUMBER,
			ACCESSLINETYPE,
			ACCESSLINEID,
			PRODUCTTYPE, 
			LINELOCATION, 
			INSTALLATIONTYPE, 
			PENDINGCEASEORDERINDICATOR, 
			MDFSITEID, 
			NUMBERRETENTIONALLOWED,
			STATUS
)
                SELECT
	FILE_ID,
	ORDERNUMBER,
	ACCESSLINETYPE,
	ACCESSLINEID,
	PRODUCTTYPE, 
	LINELOCATION, 
	INSTALLATIONTYPE, 
	PENDINGCEASEORDERINDICATOR, 
	MDFSITEID, 
	NUMBERRETENTIONALLOWED,
'NEW' AS STATUS
                FROM
                    VW_WORKING_LINES
                WHERE
                    FILE_ID = v_file_id;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD Working LINES from JSON Source file','PRC_SRC_TOD_WORKING_LINES','TOD ONTDETAILS records loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD Working LINES from JSON Source file','PRC_SRC_TOD_WORKING_LINES','TOD ONTDETAILS records already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD Working LINES from JSON Source file','PRC_SRC_TOD_WORKING_LINES','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_TOD_WORKING_LINES', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_TOD_WORKING_LINES;


PROCEDURE PRC_SRC_TOD_STOPPED_LINES(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD Stopped LINES from JSON Source file','PRC_SRC_TOD_STOPPED_LINES','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_STOPPED_LINES
        WHERE
          FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_STOPPED_LINES 
(
		FILE_ID, 
		ORDERNUMBER, 
		ACCESSLINETYPE,
		ACCESSLINEID, 
		PRODUCTTYPE, 
		LINELOCATION, 
		INSTALLATIONTYPE, 
		MDFSITEID, 
		LASTWORKINGPARTIALDN, 
		STOPPEDDATE,
			STATUS
)
                SELECT
		FILE_ID, 
		ORDERNUMBER, 
		ACCESSLINETYPE,
		ACCESSLINEID, 
		PRODUCTTYPE, 
		LINELOCATION, 
		INSTALLATIONTYPE, 
		MDFSITEID, 
		LASTWORKINGPARTIALDN, 
		STOPPEDDATE,
		'NEW' AS STATUS
                FROM
                    VW_STOPPED_LINES
                WHERE
                    FILE_ID = v_file_id;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD Stopped LINES from JSON Source file','PRC_SRC_TOD_STOPPED_LINES','TOD ONTDETAILS records loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD Stopped LINES from JSON Source file','PRC_SRC_TOD_STOPPED_LINES','TOD ONTDETAILS records already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the TOD Stopped LINES from JSON Source file','PRC_SRC_TOD_STOPPED_LINES','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_TOD_STOPPED_LINES', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_SRC_TOD_STOPPED_LINES;

-- Regrade changes start 
PROCEDURE PRC_SRC_BILLING_ACCOUNT(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER)
AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD Account ID / Brand from JSON Source file','PRC_SRC_BILLING_ACCOUNT','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_BILLING_ACCOUNT
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_BILLING_ACCOUNT 
				(
				FILE_ID,
				ORD_ACC_ID,
				ORDERNUMBER,
				ACCOUNTID,
				BRAND, 
				STATUS
				)
			SELECT
				FILE_ID,
				ORD_ACC_ID,
				ORDERNUMBER,
				ACCOUNTID,
				BRAND,
				'NEW' AS STATUS
                FROM
                   VW_BILLING_ACCOUNT
                WHERE
                    FILE_ID = V_FILE_ID;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD Account ID / Brand from JSON Source file','PRC_SRC_BILLING_ACCOUNT','Account ID / Brand loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD Account ID / Brand from JSON Source file','PRC_SRC_BILLING_ACCOUNT','Account ID / Brand records already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;

		PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD Account ID / Brand from JSON Source file','PRC_SRC_BILLING_ACCOUNT','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
	PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_SRC_BILLING_ACCOUNT', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);


END PRC_SRC_BILLING_ACCOUNT;



PROCEDURE PRC_TMF_ORDERPRODUCT(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_TMF_ORDERPRODUCT','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_TMF_ORDERPRODUCT
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_TMF_ORDERPRODUCT (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				DESCRIPTION,
				NAME,
				STARTDATE,
				EFFECTIVESTARTDATE,
				PRODUCTSTATUS,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONNAME,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTSPECIFICATIONFAMILY,
				PRODUCTIDENTIFIER,
				BILLINGACCOUNT,
				ISBUNDLE,
                CONSIDER_FOR_REGRADE,
                PARENTPRODUCTSPECIFICATIONID,
				STATUS
            )
                SELECT
					FILE_ID,
					ord_prod_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					DESCRIPTION,
					NAME,
					STARTDATE,
					EFFECTIVESTARTDATE,
					STATUS,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONNAME,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTSPECIFICATIONFAMILY,
					PRODUCTIDENTIFIER,
					BILLINGACCOUNT,
					ISBUNDLE,
                    'N',
                    PARENTPRODUCTSPECIFICATIONID,
					'NEW'
                FROM
                    VW_TMF_ORDERPRODUCT
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_prod_id IS NOT NULL;

					INSERT INTO NY_MIG_TMF_ORDERPRODUCT (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				DESCRIPTION,
				NAME,
				STARTDATE,
				EFFECTIVESTARTDATE,
				PRODUCTSTATUS,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONNAME,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTSPECIFICATIONFAMILY,
				PRODUCTIDENTIFIER,
				BILLINGACCOUNT,
				ISBUNDLE,
                CONSIDER_FOR_REGRADE,
                PARENTPRODUCTSPECIFICATIONID,
				STATUS
            )
                SELECT
					FILE_ID,
					ord_prod_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					DESCRIPTION,
					NAME,
					STARTDATE,
					EFFECTIVESTARTDATE,
					STATUS,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONNAME,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTSPECIFICATIONFAMILY,
					PRODUCTIDENTIFIER,
					BILLINGACCOUNT,
					ISBUNDLE,
                    'N',
                    PARENTPRODUCTSPECIFICATIONID,
					'NEW'
                FROM
                    VW_TMF_ORDERPRODUCT_c1
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_prod_id IS NOT NULL;

					INSERT INTO NY_MIG_TMF_ORDERPRODUCT (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				DESCRIPTION,
				NAME,
				STARTDATE,
				EFFECTIVESTARTDATE,
				PRODUCTSTATUS,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONNAME,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTSPECIFICATIONFAMILY,
				PRODUCTIDENTIFIER,
				BILLINGACCOUNT,
				ISBUNDLE,
                CONSIDER_FOR_REGRADE,
                PARENTPRODUCTSPECIFICATIONID,
				STATUS
            )
                SELECT
					FILE_ID,
					ord_prod_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					DESCRIPTION,
					NAME,
					STARTDATE,
					EFFECTIVESTARTDATE,
					STATUS,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONNAME,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTSPECIFICATIONFAMILY,
					PRODUCTIDENTIFIER,
					BILLINGACCOUNT,
					ISBUNDLE,
                    'N',
                    PARENTPRODUCTSPECIFICATIONID,
					'NEW'
                FROM
                    VW_TMF_ORDERPRODUCT_c2
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_prod_id IS NOT NULL;

					INSERT INTO NY_MIG_TMF_ORDERPRODUCT (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				DESCRIPTION,
				NAME,
				STARTDATE,
				EFFECTIVESTARTDATE,
				PRODUCTSTATUS,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONNAME,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTSPECIFICATIONFAMILY,
				PRODUCTIDENTIFIER,
				BILLINGACCOUNT,
				ISBUNDLE,
                CONSIDER_FOR_REGRADE,
                PARENTPRODUCTSPECIFICATIONID,
				STATUS
            )
                SELECT
					FILE_ID,
					ord_prod_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					DESCRIPTION,
					NAME,
					STARTDATE,
					EFFECTIVESTARTDATE,
					STATUS,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONNAME,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTSPECIFICATIONFAMILY,
					PRODUCTIDENTIFIER,
					BILLINGACCOUNT,
					ISBUNDLE,
                    'N',
                    PARENTPRODUCTSPECIFICATIONID,
					'NEW'
                FROM
                    VW_TMF_ORDERPRODUCT_c3   /* added for 4th level Regrade logic */
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_prod_id IS NOT NULL;
            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_ORDERPRODUCT','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_TMF_ORDERPRODUCT','Order product already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;


PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_TMF_ORDERPRODUCT','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_TMF_ORDERPRODUCT', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_TMF_ORDERPRODUCT;

PROCEDURE PRC_TMF_TMF_PRODUCTCHARACTERISTIC(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER)AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the product characteristics from JSON Source file','PRC_TMF_TMF_PRODUCTCHARACTERISTIC','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_TMF_PRODUCTCHARACTERISTIC
        WHERE
           FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_TMF_PRODUCTCHARACTERISTIC (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTCHARACTERISTICNAME,
				PRODUCTCHARACTERISTICVALUE,
				STATUS,
                PARENTPRODUCTSPECIFICATIONID
            )
                SELECT
					FILE_ID,
					ord_char_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTCHARACTERISTICNAME,
					PRODUCTCHARACTERISTICVALUE,
					'NEW', 
                    PARENTPRODUCTSPECIFICATIONID
                FROM
                    VW_TMF_PRODUCTCHARACTERISTIC
                WHERE
                    FILE_ID = V_FILE_ID
					and ord_char_id IS NOT NULL;

			INSERT INTO NY_MIG_TMF_PRODUCTCHARACTERISTIC (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTCHARACTERISTICNAME,
				PRODUCTCHARACTERISTICVALUE,
				STATUS, 
                PARENTPRODUCTSPECIFICATIONID
            )
                SELECT
					FILE_ID,
					ord_char_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTCHARACTERISTICNAME,
					PRODUCTCHARACTERISTICVALUE,
					'NEW', 
                    PARENTPRODUCTSPECIFICATIONID
                FROM
                    VW_TMF_PRODUCTCHARACTERISTIC_c1
                WHERE
                    FILE_ID = V_FILE_ID
					and ord_char_id IS NOT NULL;

			INSERT INTO NY_MIG_TMF_PRODUCTCHARACTERISTIC (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTCHARACTERISTICNAME,
				PRODUCTCHARACTERISTICVALUE,
				STATUS, 
                PARENTPRODUCTSPECIFICATIONID
            )
                SELECT
					FILE_ID,
					ord_char_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTCHARACTERISTICNAME,
					PRODUCTCHARACTERISTICVALUE,
					'NEW', 
                    PARENTPRODUCTSPECIFICATIONID
                FROM
                    VW_TMF_PRODUCTCHARACTERISTIC_c2
                WHERE
                    FILE_ID = V_FILE_ID
					and ord_char_id IS NOT NULL;

							INSERT INTO NY_MIG_TMF_PRODUCTCHARACTERISTIC (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTCHARACTERISTICNAME,
				PRODUCTCHARACTERISTICVALUE,
				STATUS, 
                PARENTPRODUCTSPECIFICATIONID
            )
                SELECT
					FILE_ID,
					ord_char_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTCHARACTERISTICNAME,
					PRODUCTCHARACTERISTICVALUE,
					'NEW', 
                    PARENTPRODUCTSPECIFICATIONID
                FROM
                    VW_TMF_PRODUCTCHARACTERISTIC_c3  /* added for 4th level Regrade logic */
                WHERE
                    FILE_ID = V_FILE_ID
					and ord_char_id IS NOT NULL;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the product characteristics from TMF JSON to Source file','PRC_TMF_TMF_PRODUCTCHARACTERISTIC','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the product characteristics from JSON Source file','PRC_TMF_TMF_PRODUCTCHARACTERISTIC','product characteristics already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;


PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_TMF_TMF_PRODUCTCHARACTERISTIC','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_TMF_TMF_PRODUCTCHARACTERISTIC', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_TMF_TMF_PRODUCTCHARACTERISTIC;

PROCEDURE PRC_TMF_PRODUCTINSTANCEIDS(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER)AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the PRODUCTINSTANCEIDS from JSON Source file','PRC_TMF_PRODUCTINSTANCEIDS','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_TMF_PRODUCTINSTANCEIDS
        WHERE
           FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_TMF_PRODUCTINSTANCEIDS (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTINSTANCENAME,
				PRODUCTINSTANCEVALUE,
				STATUS, 
                PARENTPRODUCTSPECIFICATIONID
            )
                SELECT
					FILE_ID,
					ord_inst_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTINSTANCENAME,
					PRODUCTINSTANCEVALUE,
					'NEW', 
                    PARENTPRODUCTSPECIFICATIONID
                FROM
                    VW_TMF_PRODUCTINSTANCEIDS
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_inst_id IS NOT NULL;

				INSERT INTO NY_MIG_TMF_PRODUCTINSTANCEIDS (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTINSTANCENAME,
				PRODUCTINSTANCEVALUE,
				STATUS, 
                PARENTPRODUCTSPECIFICATIONID
            )
                SELECT
					FILE_ID,
					ord_inst_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTINSTANCENAME,
					PRODUCTINSTANCEVALUE,
					'NEW', 
                    PARENTPRODUCTSPECIFICATIONID
                FROM
                    VW_TMF_PRODUCTINSTANCEIDS_c1
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_inst_id IS NOT NULL;

			INSERT INTO NY_MIG_TMF_PRODUCTINSTANCEIDS (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTINSTANCENAME,
				PRODUCTINSTANCEVALUE,
				STATUS, 
                PARENTPRODUCTSPECIFICATIONID
            )
                SELECT
					FILE_ID,
					ord_inst_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTINSTANCENAME,
					PRODUCTINSTANCEVALUE,
					'NEW',
                    PARENTPRODUCTSPECIFICATIONID
                FROM
                    VW_TMF_PRODUCTINSTANCEIDS_c2
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_inst_id IS NOT NULL;

					INSERT INTO NY_MIG_TMF_PRODUCTINSTANCEIDS (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTINSTANCENAME,
				PRODUCTINSTANCEVALUE,
				STATUS, 
                PARENTPRODUCTSPECIFICATIONID
            )
                SELECT
					FILE_ID,
					ord_inst_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTINSTANCENAME,
					PRODUCTINSTANCEVALUE,
					'NEW', 
                	PARENTPRODUCTSPECIFICATIONID
                FROM
                    VW_TMF_PRODUCTINSTANCEIDS_c3   /* added for 4th level Regrade logic */
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_inst_id IS NOT NULL;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the PRODUCTINSTANCEIDS from TMF JSON to Source file','PRC_TMF_PRODUCTINSTANCEIDS','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the PRODUCTINSTANCEIDS from JSON Source file','PRC_TMF_PRODUCTINSTANCEIDS','Order product already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;


PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the PRODUCTINSTANCEIDS from JSON Source file','PRC_TMF_PRODUCTINSTANCEIDS','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_TMF_PRODUCTINSTANCEIDS', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_TMF_PRODUCTINSTANCEIDS;

PROCEDURE PRC_TMF_RELATEDPARTY(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER)AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_TMF_relatedparty','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_TMF_RELATEDPARTY
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_TMF_RELATEDPARTY (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTRELATEDPARTYID,
				PRODUCTRELATEDPARTYREFERREDTYPE,
				STATUS
            )
                SELECT
					FILE_ID,
					ord_party_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTRELATEDPARTYID,
					PRODUCTRELATEDPARTYREFERREDTYPE,
					'NEW'
                FROM
                    VW_TMF_RELATEDPARTY
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_party_id IS NOT NULL;

			INSERT INTO NY_MIG_TMF_RELATEDPARTY (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTRELATEDPARTYID,
				PRODUCTRELATEDPARTYREFERREDTYPE,
				STATUS
            )
                SELECT
					FILE_ID,
					ord_party_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTRELATEDPARTYID,
					PRODUCTRELATEDPARTYREFERREDTYPE,
					'NEW'
                FROM
                    VW_TMF_RELATEDPARTY_c1
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_party_id IS NOT NULL;

			INSERT INTO NY_MIG_TMF_RELATEDPARTY (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTRELATEDPARTYID,
				PRODUCTRELATEDPARTYREFERREDTYPE,
				STATUS
            )
                SELECT
					FILE_ID,
					ord_party_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTRELATEDPARTYID,
					PRODUCTRELATEDPARTYREFERREDTYPE,
					'NEW'
                FROM
                    VW_TMF_RELATEDPARTY_c2
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_party_id IS NOT NULL;

					INSERT INTO NY_MIG_TMF_RELATEDPARTY (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PRODUCTRELATEDPARTYID,
				PRODUCTRELATEDPARTYREFERREDTYPE,
				STATUS
            )
                SELECT
					FILE_ID,
					ord_party_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PRODUCTRELATEDPARTYID,
					PRODUCTRELATEDPARTYREFERREDTYPE,
					'NEW'
                FROM
                    VW_TMF_RELATEDPARTY_c3    /* added for 4th level Regrade logic */
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_party_id IS NOT NULL;

            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_relatedparty','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_TMF_relatedparty','Order product already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;


PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_TMF_relatedparty','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_TMF_relatedparty', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_TMF_relatedparty;

PROCEDURE PRC_TMF_place(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER)AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT         NUMBER := 0;
BEGIN

PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_TMF_place','Procedure - start','NEW', v_log_message, SYSDATE );

        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_TMF_PLACE
        WHERE
            FILE_ID = V_FILE_ID 
			AND STATUS = 'NEW';

        IF V_CNT = 0 THEN
            INSERT INTO NY_MIG_TMF_PLACE (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PLACEID,
				PLACEROLE,
				PLACENAME,
				STATUS
            )
                SELECT
					FILE_ID,
					ord_place_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PLACEID,
					PLACEROLE,
					PLACENAME,
					'NEW'
                FROM
                    VW_TMF_PLACE
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_place_id IS NOT NULL;

			INSERT INTO NY_MIG_TMF_PLACE (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PLACEID,
				PLACEROLE,
				PLACENAME,
				STATUS
            )
                SELECT
					FILE_ID,
					ord_place_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PLACEID,
					PLACEROLE,
					PLACENAME,
					'NEW'
                FROM
                    VW_TMF_PLACE_c1
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_place_id IS NOT NULL;

			INSERT INTO NY_MIG_TMF_PLACE (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PLACEID,
				PLACEROLE,
				PLACENAME,
				STATUS
            )
                SELECT
					FILE_ID,
					ord_place_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PLACEID,
					PLACEROLE,
					PLACENAME,
					'NEW'
                FROM
                    VW_TMF_PLACE_c2
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_place_id IS NOT NULL;

					INSERT INTO NY_MIG_TMF_PLACE (
				FILE_ID,
				ORDER_ID,
				ORDERNUMBER,
				BILLINGACCOUNTNO,
				PRODUCTID,
				PRODUCTSPECIFICATIONID,
				PRODUCTSPECIFICATIONPRODUCTTYPE,
				PLACEID,
				PLACEROLE,
				PLACENAME,
				STATUS
            )
                SELECT
					FILE_ID,
					ord_place_id,
					ORDERNUMBER,
					BILLINGACCOUNTNO,
					ORDERPRODUCTID,
					PRODUCTSPECIFICATIONID,
					PRODUCTSPECIFICATIONPRODUCTTYPE,
					PLACEID,
					PLACEROLE,
					PLACENAME,
					'NEW'
                FROM
                    VW_TMF_PLACE_c3   /* added for 4th level Regrade logic */
                WHERE
                    FILE_ID = V_FILE_ID
					AND ord_place_id IS NOT NULL;
            COMMIT;

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_place','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		ELSIF V_CNT > 0 THEN

			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_TMF_place','Order product already exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

        END IF;


PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from JSON Source file','PRC_TMF_place','Procedure - end','NEW', v_log_message, SYSDATE );

EXCEPTION
WHEN OTHERS THEN
PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_TMF_place', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);

END PRC_TMF_place;


PROCEDURE PRC_TMF_UPDATE_PRODUCT_TREE(V_ORDERNUMBER IN VARCHAR2,V_FILE_ID IN NUMBER,V_REGRADE_TYPE IN VARCHAR2) AS

    v_log_message     VARCHAR2(4000);
    V_ERR_LOG_MSG     VARCHAR2(1000);
    V_CNT			  NUMBER := 0;
    V_BRT_CNT         NUMBER := 0;
    V_BRT_CNT2        NUMBER := 0; 
    V_BRT_CNT3        NUMBER := 0; 
    V_BRT_CNT4        NUMBER := 0; 
    V_BRT_CNT5       NUMBER := 0;
    V_BRT_CNT6       NUMBER := 0;
    V_BRT_CNT7       NUMBER := 0;
    V_BRT_CNT8       NUMBER := 0;
    V_BRT_CNT9       NUMBER := 0;
    V_BRT_CNT10      NUMBER := 0;
	V_NY_PT_COUNT	  NUMBER := 0;
	V_NY_PT_COUNT1	  NUMBER := 0;	
	V_NY_PT_COUNT2	  NUMBER := 0;
	V_NY_PT_COUNT3    NUMBER := 0;    /* Added for 4th level */
	V_DELETE_CNT  	  NUMBER := 0;
    V_VISION_PRODUCT_CNT NUMBER := 0;  
    V_BASE_ACTION    VARCHAR2(100);
    TYPE tbl_PR_ID IS        TABLE OF NY_MIG_TMF_ORDERPRODUCT.PARENTPRODUCTSPECIFICATIONID%TYPE;
    l_prod_spec_id 			 tbl_PR_ID;
    l_parent_prod_spec_id 	 tbl_PR_ID;
    V_CONT_DISC_TYPE_CNT     NUMBER := 0;
    V_OFFER_LIST       	VARCHAR2(1000); 

BEGIN
IF V_REGRADE_TYPE = 'BT' THEN 
	PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Update PRODUCT TREE table for regrade','PRC_TMF_UPDATE_PRODUCT_TREE','Procedure - start','NEW', v_log_message, SYSDATE );
	        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_TMF_ORDERPRODUCT
        WHERE
            FILE_ID = V_FILE_ID 
			AND UPPER(PRODUCTIDENTIFIER) <> 'DYNAMICUPFRONTCHARGE'
			AND STATUS = 'NEW';
PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' V_CNT'|| V_CNT, SYSDATE );
        IF V_CNT > 0 THEN


        SELECT PAR_NAME
        INTO V_OFFER_LIST
        FROM NAYAN_CONFIG_PARAMS
        WHERE PAR_TYPE ='DISCOUNT_OFFER_LIST';    
PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' V_OFFER_LIST'|| V_OFFER_LIST, SYSDATE );

		FOR I IN (SELECT * FROM NY_MIG_TMF_ORDERPRODUCT 
			WHERE PARENTPRODUCTSPECIFICATIONID = PRODUCTSPECIFICATIONID
			AND FILE_ID = V_FILE_ID 
			AND UPPER(PRODUCTIDENTIFIER) <> 'DYNAMICUPFRONTCHARGE'
			-- Add check with DM 
			)
		LOOP 

   PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| I.PRODUCTSPECIFICATIONID, SYSDATE );


			SELECT COUNT(1)
			INTO V_NY_PT_COUNT
			FROM NY_MIG_PRODUCT_TREE PT
			WHERE EXISTS (SELECT 
							1 
						FROM 
							NY_MIG_TMF_ORDERPRODUCT TM
						WHERE TM.PRODUCTSPECIFICATIONID = PT.S_CODE
						AND TM.FILE_ID = V_FILE_ID 
						AND PRODUCTSPECIFICATIONID = I.PRODUCTSPECIFICATIONID
						)
			AND FILE_ID = V_FILE_ID 	
			AND S_CODE = I.PRODUCTSPECIFICATIONID
			AND STATUS = 'NEW';	

            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| I.PRODUCTSPECIFICATIONID|| ' V_NY_PT_COUNT '|| V_NY_PT_COUNT, SYSDATE );

			IF V_NY_PT_COUNT >0 THEN 
			-- Update action

				UPDATE 
						NY_MIG_PRODUCT_TREE PT
					SET PT.OV_ACTION = 'Update' , 
                    PT.PARENTPRODUCTSPECIFICATIONID = I.PARENTPRODUCTSPECIFICATIONID 
				WHERE FILE_ID = V_FILE_ID 	
				AND S_CODE = I.PRODUCTSPECIFICATIONID;

                 /* BRITBOX , APPLETV AND HOME SECURITY changes added by RUDHRA KUMAR :: STARTS */

                ELSE
                          SELECT COUNT(*)
                            INTO V_BRT_CNT7
                            FROM NY_MIG_PRODUCT_TREE
                            WHERE PRODUCT_ID IN ( 'N0001682' ,'N0001651','N0001659','N0001664','N0001662' )
                            AND FILE_ID = V_FILE_ID;    
                  IF 
                    

                       ( i.PRODUCTSPECIFICATIONID  IN ('S0579424','S0579464','S0579504','S0579344','S0579384') and ( V_BRT_CNT7 = 0 OR V_BRT_CNT7 > 0 )) THEN

                            DELETE FROM NY_MIG_PRODUCT_TREE 
                            WHERE S_CODE IN ('S0579424','S0579464','S0579504','S0579344','S0579384')
                            AND FILE_ID = V_FILE_ID;

                 /* BRITBOX , APPLETV AND HOME SECURITY changes added by RUDHRA KUMAR :: ENDS */

			ELSE 
				-- DELETE 
				INSERT INTO NY_MIG_PRODUCT_TREE (	
						/*    1    */	MAPPING_TYPE,	
						/*    2    */	PRODUCT_TYPE,	
						/*    3    */	PRODUCT_ID,	
						/*    4    */	S_CODE,	
						/*    5    */	NAME,	
						/*    6    */	LINK_TO_SERVICE_POINT,	
						/*    7    */	CONTRACT_TERM,	
						/*    8    */	PARENT_S_CODE,	
						/*    9    */	PARENT,	
						/*    10    */	RULES,	
						/*    11    */	PRODUCT_FAMILY,	
						/*    12    */	ATTRIBUTE_TYPE,	
						/*    13    */	ATTRIBUTE_DISPLAY_NAME,	
						/*    14    */	UNITPRICE,	
						/*    15    */	DVCPE_DEVICE_QUANTITY,	
						/*    16    */	DISCOUNTAMOUNT,	
						/*    17    */	PRODUCTTERM,	
						/*    18    */	PRODUCTTERMUNIT,	
						/*    19    */	FILE_ID,	
						/*    20    */	ORD_PROD_ID,	
						/*    21    */	ORDERNUMBER,	
						/*    22    */	BILLINGACCOUNTNO,	
						/*    23    */	APPOINTMENT_REFERENCE__C,	
						/*    24    */	APPOINTMENTENDDATE,	
						/*    25    */	APPOINTMENTSTATUS,	
						/*    26    */	APPOINTMENTSTARTDATE,	
						/*    27    */	APPOINTMENTCREATEDDATE,	
						/*    28    */	APPOINTMENTPRODUCTTYPE,	
						/*    29    */	VLOCITY_CMT__ONETIMECHARGE__C,	
						/*    30    */	VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,	
						/*    31    */	PRODUCTNAME,	
						/*    32    */	VLOCITY_CMT__LINENUMBER__C,	
						/*    33    */	VLOCITY_CMT__FULFILMENTSTATUS__C,	
						/*    34    */	VLOCITY_CMT__ISPONRREACHED__C,	
						/*    35    */	VLOCITY_CMT__PROVISIONINGSTATUS__C,	
						/*    36    */	PRODUCTCODE,	
						/*    37    */	PRODUCTID,	
						/*    38    */	NG_CUSTOMERREQUESTEDDATE__C,	
						/*    39    */	VLOCITY_CMT__RECURRINGCHARGE__C,	
						/*    40    */	QUANTITY,	
						/*    41    */	VLOCITY_CMT__RECURRINGTOTAL__C,	
						/*    42    */	ORDERITEMNUMBER,	
						/*    43    */	VLOCITY_CMT__ACTION__C,	
						/*    44    */	COM_CALC_RECURRINGCHARGE__C,	
						/*    45    */	DELIVERY_INSTRUCTIONS__C,	
						/*    46    */	STATUS  ,
						/*    47    */  OV_ACTION,
                        /*    48    */  VLOCITY_CMT__SUBACTION__C, 
						/*    49    */ PARENTPRODUCTSPECIFICATIONID
						)
				SELECT 
					/*    1    */ 'root', 
					/*    2    */ '' AS PRODUCT_TYPE,--I.PRODUCTSPECIFICATIONPRODUCTTYPE AS PRODUCT_TYPE,  
					/*    3    */ '' AS PRODUCT_ID,--I.PRODUCTIDENTIFIER AS PRODUCT_ID, 
					/*    4    */ I.PRODUCTSPECIFICATIONID,
					/*    5    */ I.PRODUCTSPECIFICATIONNAME name, 
					/*    6    */ 'N' AS LINK_TO_SERVICE_POINT,
					/*    7    */ '' AS CONTRACT_TERM, 
					/*    8    */ I.PRODUCTSPECIFICATIONID, 
					/*    9    */ I.PRODUCTSPECIFICATIONNAME, 
					/*    10    */ '1' AS RULES, 
					/*    11    */ '' AS PRODUCT_FAMILY, -- I.PRODUCTSPECIFICATIONFAMILY as PRODUCT_FAMILY, 
					/*    12    */ '' AS ATTRIBUTE_TYPE, 
					/*    13    */ '' AS ATTRIBUTE_DISPLAY_NAME, 
					/*    14    */ '' AS  unitPrice,
					/*    15    */ '' DVCPE_DEVICE_QUANTITY, 
					/*    16    */ '' DISCOUNTAMOUNT, 
					/*    17    */ '' PRODUCTTERM, 
					/*    18    */ '' PRODUCTTERMUNIT, 
					/*    19    */ I.FILE_ID, 
					/*    20    */ I.ORDER_ID, 
					/*    21    */ I.ORDERNUMBER, 
					/*    22    */ I.BILLINGACCOUNTNO, 
					/*    23    */ '' APPOINTMENT_REFERENCE__C,--OP.APPOINTMENT_REFERENCE__C, 
					/*    24    */ '' APPOINTMENTENDDATE, -- OP.APPOINTMENTENDDATE, 
					/*    25    */ '' APPOINTMENTSTATUS,--OP.APPOINTMENTSTATUS, 
					/*    26    */ '' APPOINTMENTSTARTDATE,--OP.APPOINTMENTSTARTDATE, 
					/*    27    */ '' APPOINTMENTCREATEDDATE,--OP.APPOINTMENTCREATEDDATE, 
					/*    28    */ '' APPOINTMENTPRODUCTTYPE,--OP.APPOINTMENTPRODUCTTYPE, 
					/*    29    */ '' VLOCITY_CMT__ONETIMECHARGE__C,--OP.VLOCITY_CMT__ONETIMECHARGE__C, 
					/*    30    */ '' VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,--OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C, 
					/*    31    */ I.NAME, --OP.PRODUCTNAME, 
					/*    32    */ '' VLOCITY_CMT__LINENUMBER__C,--OP.VLOCITY_CMT__LINENUMBER__C, 
					/*    33    */ '' VLOCITY_CMT__FULFILMENTSTATUS__C,--OP.VLOCITY_CMT__FULFILMENTSTATUS__C, 
					/*    34    */ '' VLOCITY_CMT__ISPONRREACHED__C,--OP.VLOCITY_CMT__ISPONRREACHED__C, 
					/*    35    */ '' VLOCITY_CMT__PROVISIONINGSTATUS__C,--OP.VLOCITY_CMT__PROVISIONINGSTATUS__C, 
					/*    36    */ '' PRODUCTCODE,--OP.PRODUCTCODE, 
					/*    37    */ I.PRODUCTID, 
					/*    38    */ '' NG_CUSTOMERREQUESTEDDATE__C,--OP.NG_CUSTOMERREQUESTEDDATE__C, 
					/*    39    */ '' VLOCITY_CMT__RECURRINGCHARGE__C,--OP.VLOCITY_CMT__RECURRINGCHARGE__C, 
					/*    40    */ '' QUANTITY,--OP.QUANTITY, 
					/*    41    */ '' VLOCITY_CMT__RECURRINGTOTAL__C,--OP.VLOCITY_CMT__RECURRINGTOTAL__C, 
					/*    42    */ '' ORDERITEMNUMBER,--OP.ORDERITEMNUMBER, 
					/*    43    */ 'Delete' as  VLOCITY_CMT__ACTION__C,--OP.VLOCITY_CMT__ACTION__C, 
					/*    44    */ '' COM_CALC_RECURRINGCHARGE__C,--OP.COM_CALC_RECURRINGCHARGE__C, 
					/*    45    */ '' DELIVERY_INSTRUCTIONS__C,--OP.DELIVERY_INSTRUCTIONS__C, 
					/*    46    */ 'NEW' ,
					/*    47    */  'Delete' as OV_ACTION        ,              /* Added by bala for N 2 N regrade */
                    /*    48    */ '' VLOCITY_CMT__SUBACTION__C, 
					/*    49    */ I.PARENTPRODUCTSPECIFICATIONID
					from 
                    DUAL;                    
					--NY_MIG_TMF_ORDERPRODUCT OP
					--WHERE 
					--OP.ORDERNUMBER = V_ORDERNUMBER
					--AND PRODUCTSPECIFICATIONID = I.PRODUCTSPECIFICATIONID
					--AND OP.CONSIDER_FOR_REGRADE = 'Y'
					--;

			END IF; --britbox condition
			END IF;

			FOR J IN (SELECT * FROM NY_MIG_TMF_ORDERPRODUCT 
					WHERE FILE_ID = V_FILE_ID 
					AND PARENTPRODUCTSPECIFICATIONID = I.PRODUCTSPECIFICATIONID
                    AND PRODUCTSPECIFICATIONID <> I.PRODUCTSPECIFICATIONID
                    AND PRODUCTSPECIFICATIONID NOT IN ('S0349018')
					)
			LOOP 
            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| J.PRODUCTSPECIFICATIONID, SYSDATE );
				SELECT COUNT(1)
				INTO V_NY_PT_COUNT1
				FROM NY_MIG_PRODUCT_TREE PT
                WHERE EXISTS (SELECT 
								1 
							FROM 
								NY_MIG_TMF_ORDERPRODUCT TM
							WHERE TM.PRODUCTSPECIFICATIONID = PT.S_CODE
							AND TM.FILE_ID = V_FILE_ID 
							AND PRODUCTSPECIFICATIONID = J.PRODUCTSPECIFICATIONID
							)
				AND PT.FILE_ID = V_FILE_ID 	
				AND PT.S_CODE = J.PRODUCTSPECIFICATIONID
				AND PT.STATUS = 'NEW';	

            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| J.PRODUCTSPECIFICATIONID|| ' V_NY_PT_COUNT1'|| V_NY_PT_COUNT1, SYSDATE );


                V_CONT_DISC_TYPE_CNT :=0;

                    SELECT COUNT(1) 
                    INTO V_CONT_DISC_TYPE_CNT
                    from NY_MIG_PRODUCT_TREE PT
                    WHERE PT.FILE_ID = V_FILE_ID 
                    AND PT.S_CODE like  j.PRODUCTSPECIFICATIONID||'%'
					AND (REGEXP_LIKE(UPPER(PT.PRODUCT_TYPE),V_OFFER_LIST)
                    OR  (REGEXP_LIKE(UPPER(PT.PRODUCT_TYPE),'BBCONTRACT|BBSWCONT|BBCHOPCONT|TVCONTRACT'))
                     -- Rule 31 
                    OR Regexp_count(PT.RULES,'31')>0  );


            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| J.PRODUCTSPECIFICATIONID|| ' V_CONT_DISC_TYPE_CNT'|| V_CONT_DISC_TYPE_CNT, SYSDATE );


				IF V_NY_PT_COUNT1 >0  and V_CONT_DISC_TYPE_CNT = 0 THEN 
				-- Update action
                    V_BASE_ACTION := 'Update';
					UPDATE 
							NY_MIG_PRODUCT_TREE PT
						SET PT.OV_ACTION = 'Update', 
                            PT.PARENTPRODUCTSPECIFICATIONID = J.PARENTPRODUCTSPECIFICATIONID  
					WHERE FILE_ID = V_FILE_ID 
					AND S_CODE = J.PRODUCTSPECIFICATIONID;


				ELSE

              /* BRITBOX , APPLETV AND HOME SECURITY changes added by RUDHRA KUMAR :: STARTS */



                         SELECT COUNT(*)
                            INTO V_BRT_CNT8
                            FROM NY_MIG_PRODUCT_TREE
                            WHERE PRODUCT_ID IN ( 'N0001682' ,'N0001651','N0001659','N0001664','N0001662' )
                            AND FILE_ID = V_FILE_ID;   
                  IF 
                     
                      ( J.PRODUCTSPECIFICATIONID  IN ('S0579424','S0579464','S0579504','S0579344','S0579384') and ( V_BRT_CNT8 = 0 OR V_BRT_CNT8 > 0 )) THEN

                            DELETE FROM NY_MIG_PRODUCT_TREE 
                            WHERE S_CODE IN ('S0579424','S0579464','S0579504','S0579344','S0579384')
                            AND FILE_ID = V_FILE_ID;        

                               /* BRITBOX , APPLETV AND HOME SECURITY changes added by RUDHRA KUMAR :: ENDS */


                  ELSE


                            -- DELETE 
                            V_BASE_ACTION := 'Delete';
                            INSERT INTO NY_MIG_PRODUCT_TREE (	
                                    /*    1    */	MAPPING_TYPE,	
                                    /*    2    */	PRODUCT_TYPE,	
                                    /*    3    */	PRODUCT_ID,	
                                    /*    4    */	S_CODE,	
                                    /*    5    */	NAME,	
                                    /*    6    */	LINK_TO_SERVICE_POINT,	
                                    /*    7    */	CONTRACT_TERM,	
                                    /*    8    */	PARENT_S_CODE,	
                                    /*    9    */	PARENT,	
                                    /*    10    */	RULES,	
                                    /*    11    */	PRODUCT_FAMILY,	
                                    /*    12    */	ATTRIBUTE_TYPE,	
                                    /*    13    */	ATTRIBUTE_DISPLAY_NAME,	
                                    /*    14    */	UNITPRICE,	
                                    /*    15    */	DVCPE_DEVICE_QUANTITY,	
                                    /*    16    */	DISCOUNTAMOUNT,	
                                    /*    17    */	PRODUCTTERM,	
                                    /*    18    */	PRODUCTTERMUNIT,	
                                    /*    19    */	FILE_ID,	
                                    /*    20    */	ORD_PROD_ID,	
                                    /*    21    */	ORDERNUMBER,	
                                    /*    22    */	BILLINGACCOUNTNO,	
                                    /*    23    */	APPOINTMENT_REFERENCE__C,	
                                    /*    24    */	APPOINTMENTENDDATE,	
                                    /*    25    */	APPOINTMENTSTATUS,	
                                    /*    26    */	APPOINTMENTSTARTDATE,	
                                    /*    27    */	APPOINTMENTCREATEDDATE,	
                                    /*    28    */	APPOINTMENTPRODUCTTYPE,	
                                    /*    29    */	VLOCITY_CMT__ONETIMECHARGE__C,	
                                    /*    30    */	VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,	
                                    /*    31    */	PRODUCTNAME,	
                                    /*    32    */	VLOCITY_CMT__LINENUMBER__C,	
                                    /*    33    */	VLOCITY_CMT__FULFILMENTSTATUS__C,	
                                    /*    34    */	VLOCITY_CMT__ISPONRREACHED__C,	
                                    /*    35    */	VLOCITY_CMT__PROVISIONINGSTATUS__C,	
                                    /*    36    */	PRODUCTCODE,	
                                    /*    37    */	PRODUCTID,	
                                    /*    38    */	NG_CUSTOMERREQUESTEDDATE__C,	
                                    /*    39    */	VLOCITY_CMT__RECURRINGCHARGE__C,	
                                    /*    40    */	QUANTITY,	
                                    /*    41    */	VLOCITY_CMT__RECURRINGTOTAL__C,	
                                    /*    42    */	ORDERITEMNUMBER,	
                                    /*    43    */	VLOCITY_CMT__ACTION__C,	
                                    /*    44    */	COM_CALC_RECURRINGCHARGE__C,	
                                    /*    45    */	DELIVERY_INSTRUCTIONS__C,	
                                    /*    46    */	STATUS  ,
                                    /*    47    */  OV_ACTION, /* added by Bala N2N for Regrade */
                                     /*   48    */  VLOCITY_CMT__SUBACTION__C, 
									 /*    49    */ PARENTPRODUCTSPECIFICATIONID
                                    )
                            SELECT 
                                /*    1    */ 'root', 
                                /*    2    */ '' AS PRODUCT_TYPE,--J.PRODUCTSPECIFICATIONPRODUCTTYPE AS PRODUCT_TYPE,  
                                /*    3    */ '' AS PRODUCT_ID,--J.PRODUCTIDENTIFIER AS PRODUCT_ID, 
                                /*    4    */ J.PRODUCTSPECIFICATIONID,
                                /*    5    */ J.PRODUCTSPECIFICATIONNAME name, 
                                /*    6    */ 'N' AS LINK_TO_SERVICE_POINT,
                                /*    7    */ '' AS CONTRACT_TERM, 
                                /*    8    */ J.PRODUCTSPECIFICATIONID, 
                                /*    9    */ J.PRODUCTSPECIFICATIONNAME, 
                                /*    10    */ '1' AS RULES, 
                                /*    11    */ '' AS PRODUCT_FAMILY, -- J.PRODUCTSPECIFICATIONFAMILY as PRODUCT_FAMILY, 
                                /*    12    */ '' AS ATTRIBUTE_TYPE, 
                                /*    13    */ '' AS ATTRIBUTE_DISPLAY_NAME, 
                                /*    14    */ '' AS  unitPrice,
                                /*    15    */ '' DVCPE_DEVICE_QUANTITY, 
                                /*    16    */ '' DISCOUNTAMOUNT, 
                                /*    17    */ '' PRODUCTTERM, 
                                /*    18    */ '' PRODUCTTERMUNIT, 
                                /*    19    */ J.FILE_ID, 
                                /*    20    */ J.ORDER_ID, 
                                /*    21    */ J.ORDERNUMBER, 
                                /*    22    */ J.BILLINGACCOUNTNO, 
                                /*    23    */ '' APPOINTMENT_REFERENCE__C,--OP.APPOINTMENT_REFERENCE__C, 
                                /*    24    */ '' APPOINTMENTENDDATE, -- OP.APPOINTMENTENDDATE, 
                                /*    25    */ '' APPOINTMENTSTATUS,--OP.APPOINTMENTSTATUS, 
                                /*    26    */ '' APPOINTMENTSTARTDATE,--OP.APPOINTMENTSTARTDATE, 
                                /*    27    */ '' APPOINTMENTCREATEDDATE,--OP.APPOINTMENTCREATEDDATE, 
                                /*    28    */ '' APPOINTMENTPRODUCTTYPE,--OP.APPOINTMENTPRODUCTTYPE, 
                                /*    29    */ '' VLOCITY_CMT__ONETIMECHARGE__C,--OP.VLOCITY_CMT__ONETIMECHARGE__C, 
                                /*    30    */ '' VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,--OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C, 
                                /*    31    */ J.NAME, --OP.PRODUCTNAME, 
                                /*    32    */ '' VLOCITY_CMT__LINENUMBER__C,--OP.VLOCITY_CMT__LINENUMBER__C, 
                                /*    33    */ '' VLOCITY_CMT__FULFILMENTSTATUS__C,--OP.VLOCITY_CMT__FULFILMENTSTATUS__C, 
                                /*    34    */ '' VLOCITY_CMT__ISPONRREACHED__C,--OP.VLOCITY_CMT__ISPONRREACHED__C, 
                                /*    35    */ '' VLOCITY_CMT__PROVISIONINGSTATUS__C,--OP.VLOCITY_CMT__PROVISIONINGSTATUS__C, 
                                /*    36    */ '' PRODUCTCODE,--OP.PRODUCTCODE, 
                                /*    37    */ J.PRODUCTID, 
                                /*    38    */ '' NG_CUSTOMERREQUESTEDDATE__C,--OP.NG_CUSTOMERREQUESTEDDATE__C, 
                                /*    39    */ '' VLOCITY_CMT__RECURRINGCHARGE__C,--OP.VLOCITY_CMT__RECURRINGCHARGE__C, 
                                /*    40    */ '' QUANTITY,--OP.QUANTITY, 
                                /*    41    */ '' VLOCITY_CMT__RECURRINGTOTAL__C,--OP.VLOCITY_CMT__RECURRINGTOTAL__C, 
                                /*    42    */ '' ORDERITEMNUMBER,--OP.ORDERITEMNUMBER, 
                                /*    43    */ 'Delete' as  VLOCITY_CMT__ACTION__C,--OP.VLOCITY_CMT__ACTION__C, 
                                /*    44    */ '' COM_CALC_RECURRINGCHARGE__C,--OP.COM_CALC_RECURRINGCHARGE__C, 
                                /*    45    */ '' DELIVERY_INSTRUCTIONS__C,--OP.DELIVERY_INSTRUCTIONS__C, 
                                /*    46    */ 'NEW' ,
                                /*    47    */ 'Delete' as OV_ACTION,
                                /*    48    */  '' VLOCITY_CMT__SUBACTION__C, 
								/*    49    */ J.PARENTPRODUCTSPECIFICATIONID

                                from
                                DUAL;
                                --NY_MIG_TMF_ORDERPRODUCT OP
                                --WHERE 
                                --OP.ORDERNUMBER = V_ORDERNUMBER
                                --AND PRODUCTSPECIFICATIONID = J.PRODUCTSPECIFICATIONID
                                --AND OP.CONSIDER_FOR_REGRADE = 'Y'
                                --;
                    END IF; --BRITBOX CONDITION 
				END IF;

				FOR K IN (SELECT * FROM NY_MIG_TMF_ORDERPRODUCT 
						WHERE FILE_ID = V_FILE_ID 
						AND PARENTPRODUCTSPECIFICATIONID = J.PRODUCTSPECIFICATIONID
                        AND PRODUCTSPECIFICATIONID NOT IN ('S0349018')
						)
				LOOP 
                    PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| K.PRODUCTSPECIFICATIONID, SYSDATE );


					/*SELECT COUNT(1)
					INTO V_NY_PT_COUNT2
					FROM NY_MIG_PRODUCT_TREE PT,
                        ny_mig_product_link LP
                    WHERE PT.ordernumber = LP.ordernumber
                    AND PT.S_CODE = LP.S_CODE
                    AND PT.parent_S_CODE = LP.parent_S_CODE
					AND EXISTS (SELECT 
									1 
								FROM 
									NY_MIG_TMF_ORDERPRODUCT TM
								WHERE TM.PRODUCTSPECIFICATIONID = PT.S_CODE
								AND TM.ORDERNUMBER = V_ORDERNUMBER
								AND PRODUCTSPECIFICATIONID = K.PRODUCTSPECIFICATIONID
                                AND TM.PARENTPRODUCTSPECIFICATIONID = LP.LINK_PRODUCT_SCODE
								)
					AND PT.ORDERNUMBER = V_ORDERNUMBER	
					AND PT.S_CODE = K.PRODUCTSPECIFICATIONID
                    AND LP.LINK_PRODUCT_SCODE  = K.PARENTPRODUCTSPECIFICATIONID
					AND PT.STATUS = 'NEW';	
                    */

                    SELECT COUNT(1)
					INTO V_NY_PT_COUNT2
					FROM NY_MIG_PRODUCT_TREE PT
                    WHERE EXISTS (SELECT 
									1 
								FROM 
									NY_MIG_TMF_ORDERPRODUCT TM
								WHERE TM.PRODUCTSPECIFICATIONID = PT.S_CODE
								AND TM.FILE_ID = V_FILE_ID 
								AND PRODUCTSPECIFICATIONID = K.PRODUCTSPECIFICATIONID
								)
					AND PT.FILE_ID = V_FILE_ID 
					AND PT.S_CODE = K.PRODUCTSPECIFICATIONID
					AND PT.STATUS = 'NEW';	

                    PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| K.PRODUCTSPECIFICATIONID|| ' V_NY_PT_COUNT2'|| V_NY_PT_COUNT2, SYSDATE );

                    V_CONT_DISC_TYPE_CNT :=0;

                    /*SELECT COUNT(1) 
                    INTO V_CONT_DISC_TYPE_CNT
                    from driver_asset_mapping DM
                    WHERE DM.S_CODE = K.PRODUCTSPECIFICATIONID
					AND (REGEXP_LIKE(UPPER(DM.PRODUCT_TYPE),V_OFFER_LIST)
                    OR  (REGEXP_LIKE(UPPER(DM.PRODUCT_TYPE),'BBCONTRACT|BBSWCONT|BBCHOPCONT|TVCONTRACT')));*/

                    SELECT COUNT(1) 
                    INTO V_CONT_DISC_TYPE_CNT
                    from NY_MIG_PRODUCT_TREE PT
                    WHERE PT.FILE_ID = V_FILE_ID 
                    AND PT.S_CODE like  K.PRODUCTSPECIFICATIONID||'%'
					AND (REGEXP_LIKE(UPPER(PT.PRODUCT_TYPE),V_OFFER_LIST)
                    OR  (REGEXP_LIKE(UPPER(PT.PRODUCT_TYPE),'BBCONTRACT|BBSWCONT|BBCHOPCONT|TVCONTRACT'))
                     -- Rule 31 
                    OR Regexp_count(PT.RULES,'31')>0  );

                    PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| K.PRODUCTSPECIFICATIONID|| ' V_CONT_DISC_TYPE_CNT'|| V_CONT_DISC_TYPE_CNT, SYSDATE );

					IF V_NY_PT_COUNT2 >0  and V_CONT_DISC_TYPE_CNT = 0 THEN 
					-- Update action

						/*UPDATE 
								NY_MIG_PRODUCT_TREE PT
							SET PT.VLOCITY_CMT__ACTION__C = 'Update' 
						WHERE ORDERNUMBER = V_ORDERNUMBER	
                        AND S_CODE = K.PRODUCTSPECIFICATIONID
                        and exists (select 1 from ny_mig_product_link LP , NY_MIG_TMF_ORDERPRODUCT TP
                                        WHERE PT.ordernumber = LP.ordernumber
                                            AND PT.S_CODE = LP.S_CODE
                                            AND PT.parent_S_CODE = LP.parent_S_CODE
                                            and TP.ORDERNUMBER = TP.ORDERNUMBER 
                                            AND TP.PRODUCTSPECIFICATIONID = LP.S_CODE
                                                    AND TP.PARENTPRODUCTSPECIFICATIONID = LP.LINK_PRODUCT_SCODE);
                      */                              
                      UPDATE 
								NY_MIG_PRODUCT_TREE PT
							SET PT.OV_ACTION = 'Update' , 
                                PT.PARENTPRODUCTSPECIFICATIONID = K.PARENTPRODUCTSPECIFICATIONID 
						WHERE FILE_ID = V_FILE_ID 	
                        AND S_CODE = K.PRODUCTSPECIFICATIONID;

					ELSE 

              /* BRITBOX , APPLETV AND HOME SECURITY changes added by RUDHRA KUMAR :: STARTS */



                        SELECT COUNT(*)
                            INTO V_BRT_CNT9
                            FROM NY_MIG_PRODUCT_TREE
                            WHERE PRODUCT_ID IN ( 'N0001682' ,'N0001651','N0001659','N0001664','N0001662' )
                            AND FILE_ID = V_FILE_ID;      


                  IF 
                       

                      ( K.PRODUCTSPECIFICATIONID  IN ('S0579424','S0579464','S0579504','S0579344','S0579384') and ( V_BRT_CNT9 = 0 OR V_BRT_CNT9 > 0 )) THEN

                            DELETE FROM NY_MIG_PRODUCT_TREE 
                            WHERE S_CODE IN ('S0579424','S0579464','S0579504','S0579344','S0579384')
                            AND FILE_ID = V_FILE_ID;

              /* BRITBOX , APPLETV AND HOME SECURITY changes added by RUDHRA KUMAR :: ENDS */


                  ELSE

						-- DELETE 

                                  INSERT INTO NY_MIG_PRODUCT_TREE (	
                                        /*    1    */	MAPPING_TYPE,	
                                        /*    2    */	PRODUCT_TYPE,	
                                        /*    3    */	PRODUCT_ID,	
                                        /*    4    */	S_CODE,	
                                        /*    5    */	NAME,	
                                        /*    6    */	LINK_TO_SERVICE_POINT,	
                                        /*    7    */	CONTRACT_TERM,	
                                        /*    8    */	PARENT_S_CODE,	
                                        /*    9    */	PARENT,	
                                        /*    10    */	RULES,	
                                        /*    11    */	PRODUCT_FAMILY,	
                                        /*    12    */	ATTRIBUTE_TYPE,	
                                        /*    13    */	ATTRIBUTE_DISPLAY_NAME,	
                                        /*    14    */	UNITPRICE,	
                                        /*    15    */	DVCPE_DEVICE_QUANTITY,	
                                        /*    16    */	DISCOUNTAMOUNT,	
                                        /*    17    */	PRODUCTTERM,	
                                        /*    18    */	PRODUCTTERMUNIT,	
                                        /*    19    */	FILE_ID,	
                                        /*    20    */	ORD_PROD_ID,	
                                        /*    21    */	ORDERNUMBER,	
                                        /*    22    */	BILLINGACCOUNTNO,	
                                        /*    23    */	APPOINTMENT_REFERENCE__C,	
                                        /*    24    */	APPOINTMENTENDDATE,	
                                        /*    25    */	APPOINTMENTSTATUS,	
                                        /*    26    */	APPOINTMENTSTARTDATE,	
                                        /*    27    */	APPOINTMENTCREATEDDATE,	
                                        /*    28    */	APPOINTMENTPRODUCTTYPE,	
                                        /*    29    */	VLOCITY_CMT__ONETIMECHARGE__C,	
                                        /*    30    */	VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,	
                                        /*    31    */	PRODUCTNAME,	
                                        /*    32    */	VLOCITY_CMT__LINENUMBER__C,	
                                        /*    33    */	VLOCITY_CMT__FULFILMENTSTATUS__C,	
                                        /*    34    */	VLOCITY_CMT__ISPONRREACHED__C,	
                                        /*    35    */	VLOCITY_CMT__PROVISIONINGSTATUS__C,	
                                        /*    36    */	PRODUCTCODE,	
                                        /*    37    */	PRODUCTID,	
                                        /*    38    */	NG_CUSTOMERREQUESTEDDATE__C,	
                                        /*    39    */	VLOCITY_CMT__RECURRINGCHARGE__C,	
                                        /*    40    */	QUANTITY,	
                                        /*    41    */	VLOCITY_CMT__RECURRINGTOTAL__C,	
                                        /*    42    */	ORDERITEMNUMBER,	
                                        /*    43    */	VLOCITY_CMT__ACTION__C,	
                                        /*    44    */	COM_CALC_RECURRINGCHARGE__C,	
                                        /*    45    */	DELIVERY_INSTRUCTIONS__C,	
                                        /*    46    */	STATUS ,
                                        /*    47    */  OV_ACTION,
                                        /*    48    */  VLOCITY_CMT__SUBACTION__C, 
										/*    49    */ PARENTPRODUCTSPECIFICATIONID
                                        )
                                SELECT 
                                    /*    1    */ CASE WHEN UPPER(K.PRODUCTRELATIONSHIPTYPE) = 'CHILD'  THEN 'child' ELSE 'root' END as MAPPING_TYPE, 
                                    /*    2    */ '' AS PRODUCT_TYPE,--K.PRODUCTSPECIFICATIONPRODUCTTYPE AS PRODUCT_TYPE,  
                                    /*    3    */ '' AS PRODUCT_ID,--K.PRODUCTIDENTIFIER AS PRODUCT_ID, 
                                    /*    4    */ K.PRODUCTSPECIFICATIONID,
                                    /*    5    */ K.PRODUCTSPECIFICATIONNAME name, 
                                    /*    6    */ 'N' AS LINK_TO_SERVICE_POINT,
                                    /*    7    */ '' AS CONTRACT_TERM, 
                                    /*    8    */ CASE WHEN UPPER(K.PRODUCTRELATIONSHIPTYPE) = 'CHILD'  THEN J.PRODUCTSPECIFICATIONID ELSE K.PRODUCTSPECIFICATIONID END ,
                                    /*    9    */ CASE WHEN UPPER(K.PRODUCTRELATIONSHIPTYPE) = 'CHILD'  THEN J.PRODUCTSPECIFICATIONNAME ELSE K.PRODUCTSPECIFICATIONNAME  END , 
                                    /*    10    */ '1' AS RULES, 
                                    /*    11    */ '' AS PRODUCT_FAMILY, -- K.PRODUCTSPECIFICATIONFAMILY as PRODUCT_FAMILY, 
                                    /*    12    */ '' AS ATTRIBUTE_TYPE, 
                                    /*    13    */ '' AS ATTRIBUTE_DISPLAY_NAME, 
                                    /*    14    */ '' AS  unitPrice,
                                    /*    15    */ '' DVCPE_DEVICE_QUANTITY, 
                                    /*    16    */ '' DISCOUNTAMOUNT, 
                                    /*    17    */ '' PRODUCTTERM, 
                                    /*    18    */ '' PRODUCTTERMUNIT, 
                                    /*    19    */ K.FILE_ID, 
                                    /*    20    */ K.ORDER_ID, 
                                    /*    21    */ K.ORDERNUMBER, 
                                    /*    22    */ K.BILLINGACCOUNTNO, 
                                    /*    23    */ '' APPOINTMENT_REFERENCE__C,--OP.APPOINTMENT_REFERENCE__C, 
                                    /*    24    */ '' APPOINTMENTENDDATE, -- OP.APPOINTMENTENDDATE, 
                                    /*    25    */ '' APPOINTMENTSTATUS,--OP.APPOINTMENTSTATUS, 
                                    /*    26    */ '' APPOINTMENTSTARTDATE,--OP.APPOINTMENTSTARTDATE, 
                                    /*    27    */ '' APPOINTMENTCREATEDDATE,--OP.APPOINTMENTCREATEDDATE, 
                                    /*    28    */ '' APPOINTMENTPRODUCTTYPE,--OP.APPOINTMENTPRODUCTTYPE, 
                                    /*    29    */ '' VLOCITY_CMT__ONETIMECHARGE__C,--OP.VLOCITY_CMT__ONETIMECHARGE__C, 
                                    /*    30    */ '' VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,--OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C, 
                                    /*    31    */ K.NAME, --OP.PRODUCTNAME, 
                                    /*    32    */ '' VLOCITY_CMT__LINENUMBER__C,--OP.VLOCITY_CMT__LINENUMBER__C, 
                                    /*    33    */ '' VLOCITY_CMT__FULFILMENTSTATUS__C,--OP.VLOCITY_CMT__FULFILMENTSTATUS__C, 
                                    /*    34    */ '' VLOCITY_CMT__ISPONRREACHED__C,--OP.VLOCITY_CMT__ISPONRREACHED__C, 
                                    /*    35    */ '' VLOCITY_CMT__PROVISIONINGSTATUS__C,--OP.VLOCITY_CMT__PROVISIONINGSTATUS__C, 
                                    /*    36    */ '' PRODUCTCODE,--OP.PRODUCTCODE, 
                                    /*    37    */ K.PRODUCTID, 
                                    /*    38    */ '' NG_CUSTOMERREQUESTEDDATE__C,--OP.NG_CUSTOMERREQUESTEDDATE__C, 
                                    /*    39    */ '' VLOCITY_CMT__RECURRINGCHARGE__C,--OP.VLOCITY_CMT__RECURRINGCHARGE__C, 
                                    /*    40    */ '' QUANTITY,--OP.QUANTITY, 
                                    /*    41    */ '' VLOCITY_CMT__RECURRINGTOTAL__C,--OP.VLOCITY_CMT__RECURRINGTOTAL__C, 
                                    /*    42    */ '' ORDERITEMNUMBER,--OP.ORDERITEMNUMBER, 
                                    /*    43    */ 'Delete' as  VLOCITY_CMT__ACTION__C,--OP.VLOCITY_CMT__ACTION__C, 
                                    /*    44    */ '' COM_CALC_RECURRINGCHARGE__C,--OP.COM_CALC_RECURRINGCHARGE__C, 
                                    /*    45    */ '' DELIVERY_INSTRUCTIONS__C,--OP.DELIVERY_INSTRUCTIONS__C, 
                                    /*    46    */ 'NEW',
                                    /*    47    */  'Delete',
                                    /*    47    */ '' VLOCITY_CMT__SUBACTION__C, 
									/*    49    */ K.PARENTPRODUCTSPECIFICATIONID
                                    from 
                                    DUAL;
                                    --NY_MIG_TMF_ORDERPRODUCT OP
                                    --WHERE 
                                    --OP.ORDERNUMBER = V_ORDERNUMBER
                                    --AND PRODUCTSPECIFICATIONID = K.PRODUCTSPECIFICATIONID
                                    --AND OP.CONSIDER_FOR_REGRADE = 'Y'
                                    --;
                       END IF; -- BRITBOX CONDITION 
					END IF;

					FOR L IN (SELECT * FROM NY_MIG_TMF_ORDERPRODUCT 
								WHERE FILE_ID = V_FILE_ID 
								AND PARENTPRODUCTSPECIFICATIONID = K.PRODUCTSPECIFICATIONID
								AND PRODUCTSPECIFICATIONID NOT IN ('S0349018')
								)                                                   /* Added for 4th level */
						LOOP 
							PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| L.PRODUCTSPECIFICATIONID, SYSDATE );


							SELECT COUNT(1)
							INTO V_NY_PT_COUNT3
							FROM NY_MIG_PRODUCT_TREE PT
												WHERE EXISTS (SELECT 
											1 
										FROM 
											NY_MIG_TMF_ORDERPRODUCT TM
										WHERE TM.PRODUCTSPECIFICATIONID = PT.S_CODE
										AND TM.FILE_ID = V_FILE_ID 
										AND PRODUCTSPECIFICATIONID = L.PRODUCTSPECIFICATIONID
										)
							AND PT.FILE_ID = V_FILE_ID 
							AND PT.S_CODE = L.PRODUCTSPECIFICATIONID
                            AND UPPER(PT.OV_ACTION) <> 'DELETE' 
							AND PT.STATUS = 'NEW';	

							PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| L.PRODUCTSPECIFICATIONID|| ' V_NY_PT_COUNT3'|| V_NY_PT_COUNT3, SYSDATE );

							V_CONT_DISC_TYPE_CNT :=0;

							SELECT COUNT(1) 
							INTO V_CONT_DISC_TYPE_CNT
							from NY_MIG_PRODUCT_TREE PT
							WHERE PT.FILE_ID = V_FILE_ID 
							AND PT.S_CODE like  L.PRODUCTSPECIFICATIONID||'%'
							AND (REGEXP_LIKE(UPPER(PT.PRODUCT_TYPE),V_OFFER_LIST)
							OR  (REGEXP_LIKE(UPPER(PT.PRODUCT_TYPE),'BBCONTRACT|BBSWCONT|BBCHOPCONT|TVCONTRACT'))
							 -- Rule 31 
							OR Regexp_count(PT.RULES,'31')>0  );                    

							PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| L.PRODUCTSPECIFICATIONID|| ' V_CONT_DISC_TYPE_CNT'|| V_CONT_DISC_TYPE_CNT, SYSDATE );

							IF V_NY_PT_COUNT3 >0  and V_CONT_DISC_TYPE_CNT = 0 THEN 
							-- Update action


																UPDATE NY_MIG_PRODUCT_TREE PT
																 SET PT.OV_ACTION = 'Update' , 
                                                                    PT.PARENTPRODUCTSPECIFICATIONID = L.PARENTPRODUCTSPECIFICATIONID 
																  WHERE FILE_ID = V_FILE_ID 
																AND S_CODE = L.PRODUCTSPECIFICATIONID;
							ELSE 
								-- DELETE 
								INSERT INTO NY_MIG_PRODUCT_TREE (	
										/*    1    */	MAPPING_TYPE,	
										/*    2    */	PRODUCT_TYPE,	
										/*    3    */	PRODUCT_ID,	
										/*    4    */	S_CODE,	
										/*    5    */	NAME,	
										/*    6    */	LINK_TO_SERVICE_POINT,	
										/*    7    */	CONTRACT_TERM,	
										/*    8    */	PARENT_S_CODE,	
										/*    9    */	PARENT,	
										/*    10    */	RULES,	
										/*    11    */	PRODUCT_FAMILY,	
										/*    12    */	ATTRIBUTE_TYPE,	
										/*    13    */	ATTRIBUTE_DISPLAY_NAME,	
										/*    14    */	UNITPRICE,	
										/*    15    */	DVCPE_DEVICE_QUANTITY,	
										/*    16    */	DISCOUNTAMOUNT,	
										/*    17    */	PRODUCTTERM,	
										/*    18    */	PRODUCTTERMUNIT,	
										/*    19    */	FILE_ID,	
										/*    20    */	ORD_PROD_ID,	
										/*    21    */	ORDERNUMBER,	
										/*    22    */	BILLINGACCOUNTNO,	
										/*    23    */	APPOINTMENT_REFERENCE__C,	
										/*    24    */	APPOINTMENTENDDATE,	
										/*    25    */	APPOINTMENTSTATUS,	
										/*    26    */	APPOINTMENTSTARTDATE,	
										/*    27    */	APPOINTMENTCREATEDDATE,	
										/*    28    */	APPOINTMENTPRODUCTTYPE,	
										/*    29    */	VLOCITY_CMT__ONETIMECHARGE__C,	
										/*    30    */	VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,	
										/*    31    */	PRODUCTNAME,	
										/*    32    */	VLOCITY_CMT__LINENUMBER__C,	
										/*    33    */	VLOCITY_CMT__FULFILMENTSTATUS__C,	
										/*    34    */	VLOCITY_CMT__ISPONRREACHED__C,	
										/*    35    */	VLOCITY_CMT__PROVISIONINGSTATUS__C,	
										/*    36    */	PRODUCTCODE,	
										/*    37    */	PRODUCTID,	
										/*    38    */	NG_CUSTOMERREQUESTEDDATE__C,	
										/*    39    */	VLOCITY_CMT__RECURRINGCHARGE__C,	
										/*    40    */	QUANTITY,	
										/*    41    */	VLOCITY_CMT__RECURRINGTOTAL__C,	
										/*    42    */	ORDERITEMNUMBER,	
										/*    43    */	VLOCITY_CMT__ACTION__C,	
										/*    44    */	COM_CALC_RECURRINGCHARGE__C,	
										/*    45    */	DELIVERY_INSTRUCTIONS__C,	
										/*    46    */	STATUS ,
										/*    47    */  OV_ACTION,
										/*    48    */  VLOCITY_CMT__SUBACTION__C, 
										/*    49    */ PARENTPRODUCTSPECIFICATIONID
										)
								SELECT 
									/*    1    */ CASE WHEN UPPER(L.PRODUCTRELATIONSHIPTYPE) = 'CHILD'  THEN 'child' ELSE 'root' END as MAPPING_TYPE, 
									/*    2    */ '' AS PRODUCT_TYPE,--L.PRODUCTSPECIFICATIONPRODUCTTYPE AS PRODUCT_TYPE,  
									/*    3    */ '' AS PRODUCT_ID,--L.PRODUCTIDENTIFIER AS PRODUCT_ID, 
									/*    4    */ L.PRODUCTSPECIFICATIONID,
									/*    5    */ L.PRODUCTSPECIFICATIONNAME name, 
									/*    6    */ 'N' AS LINK_TO_SERVICE_POINT,
									/*    7    */ '' AS CONTRACT_TERM, 
									/*    8    */ CASE WHEN UPPER(L.PRODUCTRELATIONSHIPTYPE) = 'CHILD'  THEN K.PRODUCTSPECIFICATIONID ELSE L.PRODUCTSPECIFICATIONID END ,
									/*    9    */ CASE WHEN UPPER(L.PRODUCTRELATIONSHIPTYPE) = 'CHILD'  THEN K.PRODUCTSPECIFICATIONNAME ELSE L.PRODUCTSPECIFICATIONNAME  END , 
									/*    10    */ '1' AS RULES, 
									/*    11    */ '' AS PRODUCT_FAMILY, -- L.PRODUCTSPECIFICATIONFAMILY as PRODUCT_FAMILY, 
									/*    12    */ '' AS ATTRIBUTE_TYPE, 
									/*    13    */ '' AS ATTRIBUTE_DISPLAY_NAME, 
									/*    14    */ '' AS  unitPrice,
									/*    15    */ '' DVCPE_DEVICE_QUANTITY, 
									/*    16    */ '' DISCOUNTAMOUNT, 
									/*    17    */ '' PRODUCTTERM, 
									/*    18    */ '' PRODUCTTERMUNIT, 
									/*    19    */ L.FILE_ID, 
									/*    20    */ L.ORDER_ID, 
									/*    21    */ L.ORDERNUMBER, 
									/*    22    */ L.BILLINGACCOUNTNO, 
									/*    23    */ '' APPOINTMENT_REFERENCE__C,--OP.APPOINTMENT_REFERENCE__C, 
									/*    24    */ '' APPOINTMENTENDDATE, -- OP.APPOINTMENTENDDATE, 
									/*    25    */ '' APPOINTMENTSTATUS,--OP.APPOINTMENTSTATUS, 
									/*    26    */ '' APPOINTMENTSTARTDATE,--OP.APPOINTMENTSTARTDATE, 
									/*    27    */ '' APPOINTMENTCREATEDDATE,--OP.APPOINTMENTCREATEDDATE, 
									/*    28    */ '' APPOINTMENTPRODUCTTYPE,--OP.APPOINTMENTPRODUCTTYPE, 
									/*    29    */ '' VLOCITY_CMT__ONETIMECHARGE__C,--OP.VLOCITY_CMT__ONETIMECHARGE__C, 
									/*    30    */ '' VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,--OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C, 
									/*    31    */ L.NAME, --OP.PRODUCTNAME, 
									/*    32    */ '' VLOCITY_CMT__LINENUMBER__C,--OP.VLOCITY_CMT__LINENUMBER__C, 
									/*    33    */ '' VLOCITY_CMT__FULFILMENTSTATUS__C,--OP.VLOCITY_CMT__FULFILMENTSTATUS__C, 
									/*    34    */ '' VLOCITY_CMT__ISPONRREACHED__C,--OP.VLOCITY_CMT__ISPONRREACHED__C, 
									/*    35    */ '' VLOCITY_CMT__PROVISIONINGSTATUS__C,--OP.VLOCITY_CMT__PROVISIONINGSTATUS__C, 
									/*    36    */ '' PRODUCTCODE,--OP.PRODUCTCODE, 
									/*    37    */ L.PRODUCTID, 
									/*    38    */ '' NG_CUSTOMERREQUESTEDDATE__C,--OP.NG_CUSTOMERREQUESTEDDATE__C, 
									/*    39    */ '' VLOCITY_CMT__RECURRINGCHARGE__C,--OP.VLOCITY_CMT__RECURRINGCHARGE__C, 
									/*    40    */ '' QUANTITY,--OP.QUANTITY, 
									/*    41    */ '' VLOCITY_CMT__RECURRINGTOTAL__C,--OP.VLOCITY_CMT__RECURRINGTOTAL__C, 
									/*    42    */ '' ORDERITEMNUMBER,--OP.ORDERITEMNUMBER, 
									/*    43    */ 'Delete' as  VLOCITY_CMT__ACTION__C,--OP.VLOCITY_CMT__ACTION__C, 
									/*    44    */ '' COM_CALC_RECURRINGCHARGE__C,--OP.COM_CALC_RECURRINGCHARGE__C, 
									/*    45    */ '' DELIVERY_INSTRUCTIONS__C,--OP.DELIVERY_INSTRUCTIONS__C, 
									/*    46    */ 'NEW',
									/*    47    */  'Delete',
									/*    47    */ '' VLOCITY_CMT__SUBACTION__C, 
									/*    49    */ L.PARENTPRODUCTSPECIFICATIONID
									from 
                                    DUAL;
									--NY_MIG_TMF_ORDERPRODUCT OP
									--WHERE 
									--OP.ORDERNUMBER = V_ORDERNUMBER
									--AND PRODUCTSPECIFICATIONID = L.PRODUCTSPECIFICATIONID
									--AND OP.CONSIDER_FOR_REGRADE = 'Y'
									--;

							END IF;
					END LOOP;   -- END OF L loop

				END LOOP; -- END OF K loop 

			END LOOP; -- END of J loop 

		END LOOP; -- End of I loop 

                -- Add marker
				INSERT INTO NY_MIG_PRODUCT_TREE (	
					/*    1    */	MAPPING_TYPE,	
					/*    2    */	PRODUCT_TYPE,	
					/*    3    */	PRODUCT_ID,	
					/*    4    */	S_CODE,	
					/*    5    */	NAME,	
					/*    6    */	LINK_TO_SERVICE_POINT,	
					/*    7    */	CONTRACT_TERM,	
					/*    8    */	PARENT_S_CODE,	
					/*    9    */	PARENT,	
					/*    10    */	RULES,	
					/*    11    */	PRODUCT_FAMILY,	
					/*    12    */	ATTRIBUTE_TYPE,	
					/*    13    */	ATTRIBUTE_DISPLAY_NAME,	
					/*    14    */	UNITPRICE,	
					/*    15    */	DVCPE_DEVICE_QUANTITY,	
					/*    16    */	DISCOUNTAMOUNT,	
					/*    17    */	PRODUCTTERM,	
					/*    18    */	PRODUCTTERMUNIT,	
					/*    19    */	FILE_ID,	
					/*    20    */	ORD_PROD_ID,	
					/*    21    */	ORDERNUMBER,	
					/*    22    */	BILLINGACCOUNTNO,	
					/*    23    */	APPOINTMENT_REFERENCE__C,	
					/*    24    */	APPOINTMENTENDDATE,	
					/*    25    */	APPOINTMENTSTATUS,	
					/*    26    */	APPOINTMENTSTARTDATE,	
					/*    27    */	APPOINTMENTCREATEDDATE,	
					/*    28    */	APPOINTMENTPRODUCTTYPE,	
					/*    29    */	VLOCITY_CMT__ONETIMECHARGE__C,	
					/*    30    */	VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,	
					/*    31    */	PRODUCTNAME,	
					/*    32    */	VLOCITY_CMT__LINENUMBER__C,	
					/*    33    */	VLOCITY_CMT__FULFILMENTSTATUS__C,	
					/*    34    */	VLOCITY_CMT__ISPONRREACHED__C,	
					/*    35    */	VLOCITY_CMT__PROVISIONINGSTATUS__C,	
					/*    36    */	PRODUCTCODE,	
					/*    37    */	PRODUCTID,	
					/*    38    */	NG_CUSTOMERREQUESTEDDATE__C,	
					/*    39    */	VLOCITY_CMT__RECURRINGCHARGE__C,	
					/*    40    */	QUANTITY,	
					/*    41    */	VLOCITY_CMT__RECURRINGTOTAL__C,	
					/*    42    */	ORDERITEMNUMBER,	
					/*    43    */	VLOCITY_CMT__ACTION__C,	
					/*    44    */	COM_CALC_RECURRINGCHARGE__C,	
					/*    45    */	DELIVERY_INSTRUCTIONS__C,	
					/*    46    */	STATUS  ,
                    /*    47    */  OV_ACTION	,
                    /*    47    */  VLOCITY_CMT__SUBACTION__C
					)	
				SELECT
					/*    1    */	AM.MAPPING_TYPE,	
					/*    2    */	AM.PRODUCT_TYPE,	
					/*    3    */	AM.PRODUCT_ID,	
					/*    4    */	AM.S_CODE,	
					/*    5    */	AM.NAME,	
					/*    6    */	AM.LINK_TO_SERVICE_POINT,	
					/*    7    */	AM.CONTRACT_TERM,	
					/*    8    */	AM.PARENT_S_CODE,	
					/*    9    */	AM.PARENT,	
					/*    10    */	AM.RULES,	
					/*    11    */	AM.PRODUCT_FAMILY,	
					/*    12    */	AM.ATTRIBUTE_TYPE,	
					/*    13    */	AM.ATTRIBUTE_DISPLAY_NAME,	
					/*    14    */	'' AS unitPrice,	
					/*    15    */	'' DVCPE_DEVICE_QUANTITY,	
					/*    16    */	'' DISCOUNTAMOUNT,	
					/*    17    */	'' PRODUCTTERM,	
					/*    18    */	'' PRODUCTTERMUNIT,	
					/*    19    */	OV.NY_FILE_ID,	
					/*    20    */	1,	
					/*    21    */	OV.ORDERNUMBER,	
					/*    22    */	OV.BILLINGACCOUNTNO,	
					/*    23    */	'' APPOINTMENT_REFERENCE__C,	
					/*    24    */	'' APPOINTMENTENDDATE, 	
					/*    25    */	'' APPOINTMENTSTATUS,	
					/*    26    */	'' APPOINTMENTSTARTDATE,	
					/*    27    */	'' APPOINTMENTCREATEDDATE,	
					/*    28    */	'' APPOINTMENTPRODUCTTYPE,	
					/*    29    */	'' VLOCITY_CMT__ONETIMECHARGE__C,	
					/*    30    */	'' VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,	
					/*    31    */	AM.NAME, 	
					/*    32    */	'' VLOCITY_CMT__LINENUMBER__C,	
					/*    33    */	'' VLOCITY_CMT__FULFILMENTSTATUS__C,	
					/*    34    */	'' VLOCITY_CMT__ISPONRREACHED__C,	
					/*    35    */	'' VLOCITY_CMT__PROVISIONINGSTATUS__C,	
					/*    36    */	'' PRODUCTCODE,	
					/*    37    */	'' PRODUCTID,	
					/*    38    */	'' NG_CUSTOMERREQUESTEDDATE__C,	
					/*    39    */	'' VLOCITY_CMT__RECURRINGCHARGE__C,	
					/*    40    */	'' QUANTITY,--OP.QUANTITY,	
					/*    41    */	'' VLOCITY_CMT__RECURRINGTOTAL__C,	
					/*    42    */	'' ORDERITEMNUMBER,	
					/*    43    */	'Add' VLOCITY_CMT__ACTION__C,	
					/*    44    */	'' COM_CALC_RECURRINGCHARGE__C,	
					/*    45    */	'' DELIVERY_INSTRUCTIONS__C,	
					/*    46    */	'NEW',
					/*    47    */  'Add' as OV_ACTION,
                    /*    47    */ '' VLOCITY_CMT__SUBACTION__C
				from 
					DRIVER_ASSET_MAPPING AM , 
					TMF_OV_RESPONSE_TABLE OV
				WHERE OV.NY_FILE_ID = V_FILE_ID
				AND AM.S_CODE = 'S0578624'
				AND UPPER(AM.MAPPING_TYPE) <> 'REDUNDANT';
        -- Logic to update link products 



		ELSE 
			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Update PRODUCT TREE table for regrade','PRC_TMF_UPDATE_PRODUCT_TREE','Order product doesnot exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		END IF;

ELSIF V_REGRADE_TYPE = 'NY' THEN 

	PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Update PRODUCT TREE table for regrade','PRC_TMF_UPDATE_PRODUCT_TREE','Procedure - start','NEW', v_log_message, SYSDATE );
	        SELECT
            COUNT(1)
        INTO V_CNT
        FROM
            NY_MIG_TMF_ORDERPRODUCT
        WHERE
            FILE_ID = V_FILE_ID 
			AND UPPER(PRODUCTIDENTIFIER) <> 'DYNAMICUPFRONTCHARGE'
			AND STATUS = 'NEW';
PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' V_CNT'|| V_CNT, SYSDATE );
        IF V_CNT > 0 THEN


        SELECT PAR_NAME
        INTO V_OFFER_LIST
        FROM NAYAN_CONFIG_PARAMS
        WHERE PAR_TYPE ='DISCOUNT_OFFER_LIST';    
PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' V_OFFER_LIST'|| V_OFFER_LIST, SYSDATE );

		FOR I IN (SELECT * FROM NY_MIG_TMF_ORDERPRODUCT 
			WHERE PARENTPRODUCTSPECIFICATIONID = PRODUCTSPECIFICATIONID
			AND FILE_ID = V_FILE_ID 
			AND UPPER(PRODUCTIDENTIFIER) <> 'DYNAMICUPFRONTCHARGE'
           -- AND NOT EXISTS (select 1 from tvcpe_addon_config b where NY_MIG_TMF_ORDERPRODUCT.s_code = b.s_code) -- psuedocode
			-- Add check with DM 
			)
		LOOP 

   PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| I.PRODUCTSPECIFICATIONID, SYSDATE );


			SELECT COUNT(1)
			INTO V_NY_PT_COUNT
			FROM NY_MIG_PRODUCT_TREE PT
			WHERE EXISTS (SELECT 
							1 
						FROM 
							NY_MIG_TMF_ORDERPRODUCT TM
						WHERE TM.PRODUCTSPECIFICATIONID = PT.S_CODE
						AND TM.FILE_ID = V_FILE_ID 
						AND PRODUCTSPECIFICATIONID = I.PRODUCTSPECIFICATIONID
						)
			AND ORDERNUMBER = V_ORDERNUMBER	
			AND S_CODE = I.PRODUCTSPECIFICATIONID
			AND STATUS = 'NEW';	

            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| I.PRODUCTSPECIFICATIONID|| ' V_NY_PT_COUNT '|| V_NY_PT_COUNT, SYSDATE );

			IF V_NY_PT_COUNT >0 THEN 
			-- Update action

				UPDATE 
						NY_MIG_PRODUCT_TREE PT
					SET PT.OV_ACTION = 'Update' , 
                        PT.PARENTPRODUCTSPECIFICATIONID = I.PARENTPRODUCTSPECIFICATIONID 
				WHERE FILE_ID = V_FILE_ID 	
				AND S_CODE = I.PRODUCTSPECIFICATIONID;

                ELSE 

                          /* BRITBOX , APPLETV AND HOME SECURITY changes added by RUDHRA KUMAR :: STARTS */


                          SELECT COUNT(*)
                            INTO V_BRT_CNT7
                            FROM NY_MIG_PRODUCT_TREE
                            WHERE PRODUCT_ID IN ( 'N0001682' ,'N0001651','N0001659','N0001664','N0001662' )
                            AND FILE_ID = V_FILE_ID;    
                  IF 
                       ( i.PRODUCTSPECIFICATIONID  IN ('S0579424','S0579464','S0579504','S0579344','S0579384') and ( V_BRT_CNT7 = 0 OR V_BRT_CNT7 > 0 )) THEN

                            DELETE FROM NY_MIG_PRODUCT_TREE 
                            WHERE S_CODE IN ('S0579424','S0579464','S0579504','S0579344','S0579384')
                            AND FILE_ID = V_FILE_ID;

               /* BRITBOX , APPLETV AND HOME SECURITY changes added by RUDHRA KUMAR :: ENDS */


                ELSE 
				-- DELETE 
				INSERT INTO NY_MIG_PRODUCT_TREE (	
						/*    1    */	MAPPING_TYPE,	
						/*    2    */	PRODUCT_TYPE,	
						/*    3    */	PRODUCT_ID,	
						/*    4    */	S_CODE,	
						/*    5    */	NAME,	
						/*    6    */	LINK_TO_SERVICE_POINT,	
						/*    7    */	CONTRACT_TERM,	
						/*    8    */	PARENT_S_CODE,	
						/*    9    */	PARENT,	
						/*    10    */	RULES,	
						/*    11    */	PRODUCT_FAMILY,	
						/*    12    */	ATTRIBUTE_TYPE,	
						/*    13    */	ATTRIBUTE_DISPLAY_NAME,	
						/*    14    */	UNITPRICE,	
						/*    15    */	DVCPE_DEVICE_QUANTITY,	
						/*    16    */	DISCOUNTAMOUNT,	
						/*    17    */	PRODUCTTERM,	
						/*    18    */	PRODUCTTERMUNIT,	
						/*    19    */	FILE_ID,	
						/*    20    */	ORD_PROD_ID,	
						/*    21    */	ORDERNUMBER,	
						/*    22    */	BILLINGACCOUNTNO,	
						/*    23    */	APPOINTMENT_REFERENCE__C,	
						/*    24    */	APPOINTMENTENDDATE,	
						/*    25    */	APPOINTMENTSTATUS,	
						/*    26    */	APPOINTMENTSTARTDATE,	
						/*    27    */	APPOINTMENTCREATEDDATE,	
						/*    28    */	APPOINTMENTPRODUCTTYPE,	
						/*    29    */	VLOCITY_CMT__ONETIMECHARGE__C,	
						/*    30    */	VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,	
						/*    31    */	PRODUCTNAME,	
						/*    32    */	VLOCITY_CMT__LINENUMBER__C,	
						/*    33    */	VLOCITY_CMT__FULFILMENTSTATUS__C,	
						/*    34    */	VLOCITY_CMT__ISPONRREACHED__C,	
						/*    35    */	VLOCITY_CMT__PROVISIONINGSTATUS__C,	
						/*    36    */	PRODUCTCODE,	
						/*    37    */	PRODUCTID,	
						/*    38    */	NG_CUSTOMERREQUESTEDDATE__C,	
						/*    39    */	VLOCITY_CMT__RECURRINGCHARGE__C,	
						/*    40    */	QUANTITY,	
						/*    41    */	VLOCITY_CMT__RECURRINGTOTAL__C,	
						/*    42    */	ORDERITEMNUMBER,	
						/*    43    */	VLOCITY_CMT__ACTION__C,	
						/*    44    */	COM_CALC_RECURRINGCHARGE__C,	
						/*    45    */	DELIVERY_INSTRUCTIONS__C,	
						/*    46    */	STATUS  ,
						/*    47    */  OV_ACTION,
                        /*    48    */  VLOCITY_CMT__SUBACTION__C, 
						/*    49    */ PARENTPRODUCTSPECIFICATIONID
						)
				SELECT 
					/*    1    */ 'root', 
					/*    2    */ '' AS PRODUCT_TYPE,--I.PRODUCTSPECIFICATIONPRODUCTTYPE AS PRODUCT_TYPE,  
					/*    3    */ '' AS PRODUCT_ID,--I.PRODUCTIDENTIFIER AS PRODUCT_ID, 
					/*    4    */ I.PRODUCTSPECIFICATIONID,
					/*    5    */ I.PRODUCTSPECIFICATIONNAME name, 
					/*    6    */ 'N' AS LINK_TO_SERVICE_POINT,
					/*    7    */ '' AS CONTRACT_TERM, 
					/*    8    */ I.PRODUCTSPECIFICATIONID, 
					/*    9    */ I.PRODUCTSPECIFICATIONNAME, 
					/*    10    */ '1' AS RULES, 
					/*    11    */ '' AS PRODUCT_FAMILY, -- I.PRODUCTSPECIFICATIONFAMILY as PRODUCT_FAMILY, 
					/*    12    */ '' AS ATTRIBUTE_TYPE, 
					/*    13    */ '' AS ATTRIBUTE_DISPLAY_NAME, 
					/*    14    */ '' AS  unitPrice,
					/*    15    */ '' DVCPE_DEVICE_QUANTITY, 
					/*    16    */ '' DISCOUNTAMOUNT, 
					/*    17    */ '' PRODUCTTERM, 
					/*    18    */ '' PRODUCTTERMUNIT, 
					/*    19    */ I.FILE_ID, 
					/*    20    */ I.ORDER_ID, 
					/*    21    */ I.ORDERNUMBER, 
					/*    22    */ I.BILLINGACCOUNTNO, 
					/*    23    */ '' APPOINTMENT_REFERENCE__C,--OP.APPOINTMENT_REFERENCE__C, 
					/*    24    */ '' APPOINTMENTENDDATE, -- OP.APPOINTMENTENDDATE, 
					/*    25    */ '' APPOINTMENTSTATUS,--OP.APPOINTMENTSTATUS, 
					/*    26    */ '' APPOINTMENTSTARTDATE,--OP.APPOINTMENTSTARTDATE, 
					/*    27    */ '' APPOINTMENTCREATEDDATE,--OP.APPOINTMENTCREATEDDATE, 
					/*    28    */ '' APPOINTMENTPRODUCTTYPE,--OP.APPOINTMENTPRODUCTTYPE, 
					/*    29    */ '' VLOCITY_CMT__ONETIMECHARGE__C,--OP.VLOCITY_CMT__ONETIMECHARGE__C, 
					/*    30    */ '' VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,--OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C, 
					/*    31    */ I.NAME, --OP.PRODUCTNAME, 
					/*    32    */ '' VLOCITY_CMT__LINENUMBER__C,--OP.VLOCITY_CMT__LINENUMBER__C, 
					/*    33    */ '' VLOCITY_CMT__FULFILMENTSTATUS__C,--OP.VLOCITY_CMT__FULFILMENTSTATUS__C, 
					/*    34    */ '' VLOCITY_CMT__ISPONRREACHED__C,--OP.VLOCITY_CMT__ISPONRREACHED__C, 
					/*    35    */ '' VLOCITY_CMT__PROVISIONINGSTATUS__C,--OP.VLOCITY_CMT__PROVISIONINGSTATUS__C, 
					/*    36    */ '' PRODUCTCODE,--OP.PRODUCTCODE, 
					/*    37    */ I.PRODUCTID, 
					/*    38    */ '' NG_CUSTOMERREQUESTEDDATE__C,--OP.NG_CUSTOMERREQUESTEDDATE__C, 
					/*    39    */ '' VLOCITY_CMT__RECURRINGCHARGE__C,--OP.VLOCITY_CMT__RECURRINGCHARGE__C, 
					/*    40    */ '' QUANTITY,--OP.QUANTITY, 
					/*    41    */ '' VLOCITY_CMT__RECURRINGTOTAL__C,--OP.VLOCITY_CMT__RECURRINGTOTAL__C, 
					/*    42    */ '' ORDERITEMNUMBER,--OP.ORDERITEMNUMBER, 
					/*    43    */ 'Delete' as  VLOCITY_CMT__ACTION__C,--OP.VLOCITY_CMT__ACTION__C, 
					/*    44    */ '' COM_CALC_RECURRINGCHARGE__C,--OP.COM_CALC_RECURRINGCHARGE__C, 
					/*    45    */ '' DELIVERY_INSTRUCTIONS__C,--OP.DELIVERY_INSTRUCTIONS__C, 
					/*    46    */ 'NEW' ,
					/*    47    */  'Delete' as OV_ACTION        ,              /* Added by bala for N 2 N regrade */
                    /*    48    */ '' VLOCITY_CMT__SUBACTION__C, 
					/*    49    */ I.PARENTPRODUCTSPECIFICATIONID
					from 
                    DUAL;
					--NY_MIG_TMF_ORDERPRODUCT OP
					--WHERE 
					--OP.ORDERNUMBER = V_ORDERNUMBER
					--AND PRODUCTSPECIFICATIONID = I.PRODUCTSPECIFICATIONID
					--AND OP.CONSIDER_FOR_REGRADE = 'Y'
					--;
              END IF;
			END IF;

			FOR J IN (SELECT * FROM NY_MIG_TMF_ORDERPRODUCT 
					WHERE FILE_ID = V_FILE_ID 
					AND PARENTPRODUCTSPECIFICATIONID = I.PRODUCTSPECIFICATIONID
                    AND PRODUCTSPECIFICATIONID <> I.PRODUCTSPECIFICATIONID
                    AND PRODUCTSPECIFICATIONID NOT IN ('S0349018')
					)
			LOOP 
            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| J.PRODUCTSPECIFICATIONID, SYSDATE );
				SELECT COUNT(1)
				INTO V_NY_PT_COUNT1
				FROM NY_MIG_PRODUCT_TREE PT
                WHERE EXISTS (SELECT 
								1 
							FROM 
								NY_MIG_TMF_ORDERPRODUCT TM
							WHERE TM.PRODUCTSPECIFICATIONID = PT.S_CODE
							AND TM.FILE_ID = V_FILE_ID 
							AND PRODUCTSPECIFICATIONID = J.PRODUCTSPECIFICATIONID
							)
				AND PT.FILE_ID = V_FILE_ID 	
				AND PT.S_CODE = J.PRODUCTSPECIFICATIONID
				AND PT.STATUS = 'NEW';	

            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| J.PRODUCTSPECIFICATIONID|| ' V_NY_PT_COUNT1'|| V_NY_PT_COUNT1, SYSDATE );


                V_CONT_DISC_TYPE_CNT :=0;

                    SELECT COUNT(1) 
                    INTO V_CONT_DISC_TYPE_CNT
                    from NY_MIG_PRODUCT_TREE PT
                    WHERE PT.FILE_ID = V_FILE_ID 
                    AND PT.S_CODE like  j.PRODUCTSPECIFICATIONID||'%'
					AND (REGEXP_LIKE(UPPER(PT.PRODUCT_TYPE),V_OFFER_LIST)
                    OR  (REGEXP_LIKE(UPPER(PT.PRODUCT_TYPE),'BBCONTRACT|BBSWCONT|BBCHOPCONT|TVCONTRACT'))
                     );

/*
                    SELECT count(*)
                    INTO V_BOX_CNT
                    FROM NY_MIG_TMF_ORDERPRODUCT TMF,
                    NY_MIG_ORDER_ATTRIBUTES OA
                    WHERE OA.ORDERNUMBER = v_ordernumber
                    AND OA.ORDERNUMBER = TMF.ORDERNUMBER
                    AND OA.ATTRIBUTENAME = 'TOGGLE'
                    AND TMF.PRODUCTSPECIFICATIONID IN ('S0507144','S0507145');
                    */

            PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| J.PRODUCTSPECIFICATIONID|| ' V_CONT_DISC_TYPE_CNT'|| V_CONT_DISC_TYPE_CNT, SYSDATE );


				IF V_NY_PT_COUNT1 >0  and V_CONT_DISC_TYPE_CNT = 0 THEN 
				-- Update action
                    V_BASE_ACTION := 'Update';
					UPDATE 
							NY_MIG_PRODUCT_TREE PT
						SET PT.OV_ACTION = 'Update', 
                            PT.PARENTPRODUCTSPECIFICATIONID = J.PARENTPRODUCTSPECIFICATIONID  
					WHERE FILE_ID = V_FILE_ID 	
					AND S_CODE = J.PRODUCTSPECIFICATIONID;
                 /*   
                 ELSIF
                      V_NY_PT_COUNT1 > 0  and V_BOX_CNT > 0 THEN

                      V_BASE_ACTION := 'Create';
					UPDATE 
							NY_MIG_PRODUCT_TREE PT
						SET PT.VLOCITY_CMT__ACTION__C = 'Create' 
					WHERE ORDERNUMBER = V_ORDERNUMBER	
					AND S_CODE = J.PRODUCTSPECIFICATIONID;
                    */

                ELSE

                              /* BRITBOX , APPLETV AND HOME SECURITY changes added by RUDHRA KUMAR :: STARTS */

                         SELECT COUNT(*)
                            INTO V_BRT_CNT8
                            FROM NY_MIG_PRODUCT_TREE
                            WHERE PRODUCT_ID IN ( 'N0001682' ,'N0001651','N0001659','N0001664','N0001662' )
                            AND FILE_ID = V_FILE_ID;   
                  IF 
                      
                      ( J.PRODUCTSPECIFICATIONID  IN ('S0579424','S0579464','S0579504','S0579344','S0579384') and ( V_BRT_CNT8 = 0 OR V_BRT_CNT8 > 0 )) THEN

                            DELETE FROM NY_MIG_PRODUCT_TREE 
                            WHERE S_CODE IN ('S0579424','S0579464','S0579504','S0579344','S0579384')
                            AND FILE_ID = V_FILE_ID;        

                            /* BRITBOX , APPLETV AND HOME SECURITY changes added by RUDHRA KUMAR :: ENDS */


                  ELSE

					-- DELETE 
					V_BASE_ACTION := 'Delete';
                    INSERT INTO NY_MIG_PRODUCT_TREE (	
							/*    1    */	MAPPING_TYPE,	
							/*    2    */	PRODUCT_TYPE,	
							/*    3    */	PRODUCT_ID,	
							/*    4    */	S_CODE,	
							/*    5    */	NAME,	
							/*    6    */	LINK_TO_SERVICE_POINT,	
							/*    7    */	CONTRACT_TERM,	
							/*    8    */	PARENT_S_CODE,	
							/*    9    */	PARENT,	
							/*    10    */	RULES,	
							/*    11    */	PRODUCT_FAMILY,	
							/*    12    */	ATTRIBUTE_TYPE,	
							/*    13    */	ATTRIBUTE_DISPLAY_NAME,	
							/*    14    */	UNITPRICE,	
							/*    15    */	DVCPE_DEVICE_QUANTITY,	
							/*    16    */	DISCOUNTAMOUNT,	
							/*    17    */	PRODUCTTERM,	
							/*    18    */	PRODUCTTERMUNIT,	
							/*    19    */	FILE_ID,	
							/*    20    */	ORD_PROD_ID,	
							/*    21    */	ORDERNUMBER,	
							/*    22    */	BILLINGACCOUNTNO,	
							/*    23    */	APPOINTMENT_REFERENCE__C,	
							/*    24    */	APPOINTMENTENDDATE,	
							/*    25    */	APPOINTMENTSTATUS,	
							/*    26    */	APPOINTMENTSTARTDATE,	
							/*    27    */	APPOINTMENTCREATEDDATE,	
							/*    28    */	APPOINTMENTPRODUCTTYPE,	
							/*    29    */	VLOCITY_CMT__ONETIMECHARGE__C,	
							/*    30    */	VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,	
							/*    31    */	PRODUCTNAME,	
							/*    32    */	VLOCITY_CMT__LINENUMBER__C,	
							/*    33    */	VLOCITY_CMT__FULFILMENTSTATUS__C,	
							/*    34    */	VLOCITY_CMT__ISPONRREACHED__C,	
							/*    35    */	VLOCITY_CMT__PROVISIONINGSTATUS__C,	
							/*    36    */	PRODUCTCODE,	
							/*    37    */	PRODUCTID,	
							/*    38    */	NG_CUSTOMERREQUESTEDDATE__C,	
							/*    39    */	VLOCITY_CMT__RECURRINGCHARGE__C,	
							/*    40    */	QUANTITY,	
							/*    41    */	VLOCITY_CMT__RECURRINGTOTAL__C,	
							/*    42    */	ORDERITEMNUMBER,	
							/*    43    */	VLOCITY_CMT__ACTION__C,	
							/*    44    */	COM_CALC_RECURRINGCHARGE__C,	
							/*    45    */	DELIVERY_INSTRUCTIONS__C,	
							/*    46    */	STATUS  ,
							/*    47    */  OV_ACTION, /* added by Bala N2N for Regrade */
                             /*   48    */  VLOCITY_CMT__SUBACTION__C, 
							 /*    49    */ PARENTPRODUCTSPECIFICATIONID
							)
					SELECT 
						/*    1    */ 'root', 
						/*    2    */ '' AS PRODUCT_TYPE,--J.PRODUCTSPECIFICATIONPRODUCTTYPE AS PRODUCT_TYPE,  
						/*    3    */ '' AS PRODUCT_ID,--J.PRODUCTIDENTIFIER AS PRODUCT_ID, 
						/*    4    */ J.PRODUCTSPECIFICATIONID,
						/*    5    */ J.PRODUCTSPECIFICATIONNAME name, 
						/*    6    */ 'N' AS LINK_TO_SERVICE_POINT,
						/*    7    */ '' AS CONTRACT_TERM, 
						/*    8    */ J.PRODUCTSPECIFICATIONID, 
						/*    9    */ J.PRODUCTSPECIFICATIONNAME, 
						/*    10    */ '1' AS RULES, 
						/*    11    */ '' AS PRODUCT_FAMILY, -- J.PRODUCTSPECIFICATIONFAMILY as PRODUCT_FAMILY, 
						/*    12    */ '' AS ATTRIBUTE_TYPE, 
						/*    13    */ '' AS ATTRIBUTE_DISPLAY_NAME, 
						/*    14    */ '' AS  unitPrice,
						/*    15    */ '' DVCPE_DEVICE_QUANTITY, 
						/*    16    */ '' DISCOUNTAMOUNT, 
						/*    17    */ '' PRODUCTTERM, 
						/*    18    */ '' PRODUCTTERMUNIT, 
						/*    19    */ J.FILE_ID, 
						/*    20    */ J.ORDER_ID, 
						/*    21    */ J.ORDERNUMBER, 
						/*    22    */ J.BILLINGACCOUNTNO, 
						/*    23    */ '' APPOINTMENT_REFERENCE__C,--OP.APPOINTMENT_REFERENCE__C, 
						/*    24    */ '' APPOINTMENTENDDATE, -- OP.APPOINTMENTENDDATE, 
						/*    25    */ '' APPOINTMENTSTATUS,--OP.APPOINTMENTSTATUS, 
						/*    26    */ '' APPOINTMENTSTARTDATE,--OP.APPOINTMENTSTARTDATE, 
						/*    27    */ '' APPOINTMENTCREATEDDATE,--OP.APPOINTMENTCREATEDDATE, 
						/*    28    */ '' APPOINTMENTPRODUCTTYPE,--OP.APPOINTMENTPRODUCTTYPE, 
						/*    29    */ '' VLOCITY_CMT__ONETIMECHARGE__C,--OP.VLOCITY_CMT__ONETIMECHARGE__C, 
						/*    30    */ '' VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,--OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C, 
						/*    31    */ J.NAME, --OP.PRODUCTNAME, 
						/*    32    */ '' VLOCITY_CMT__LINENUMBER__C,--OP.VLOCITY_CMT__LINENUMBER__C, 
						/*    33    */ '' VLOCITY_CMT__FULFILMENTSTATUS__C,--OP.VLOCITY_CMT__FULFILMENTSTATUS__C, 
						/*    34    */ '' VLOCITY_CMT__ISPONRREACHED__C,--OP.VLOCITY_CMT__ISPONRREACHED__C, 
						/*    35    */ '' VLOCITY_CMT__PROVISIONINGSTATUS__C,--OP.VLOCITY_CMT__PROVISIONINGSTATUS__C, 
						/*    36    */ '' PRODUCTCODE,--OP.PRODUCTCODE, 
						/*    37    */ J.PRODUCTID, 
						/*    38    */ '' NG_CUSTOMERREQUESTEDDATE__C,--OP.NG_CUSTOMERREQUESTEDDATE__C, 
						/*    39    */ '' VLOCITY_CMT__RECURRINGCHARGE__C,--OP.VLOCITY_CMT__RECURRINGCHARGE__C, 
						/*    40    */ '' QUANTITY,--OP.QUANTITY, 
						/*    41    */ '' VLOCITY_CMT__RECURRINGTOTAL__C,--OP.VLOCITY_CMT__RECURRINGTOTAL__C, 
						/*    42    */ '' ORDERITEMNUMBER,--OP.ORDERITEMNUMBER, 
						/*    43    */ 'Delete' as  VLOCITY_CMT__ACTION__C,--OP.VLOCITY_CMT__ACTION__C, 
						/*    44    */ '' COM_CALC_RECURRINGCHARGE__C,--OP.COM_CALC_RECURRINGCHARGE__C, 
						/*    45    */ '' DELIVERY_INSTRUCTIONS__C,--OP.DELIVERY_INSTRUCTIONS__C, 
						/*    46    */ 'NEW' ,
						/*    47    */ 'Delete' as OV_ACTION,
                        /*    48    */  '' VLOCITY_CMT__SUBACTION__C, 
						/*    49    */ J.PARENTPRODUCTSPECIFICATIONID

						from 
                        DUAL;
						--NY_MIG_TMF_ORDERPRODUCT OP
						--WHERE 
						--OP.ORDERNUMBER = V_ORDERNUMBER
						--AND PRODUCTSPECIFICATIONID = J.PRODUCTSPECIFICATIONID
						--AND OP.CONSIDER_FOR_REGRADE = 'Y'
						--;
                     END IF;
				END IF;

				FOR K IN (SELECT * FROM NY_MIG_TMF_ORDERPRODUCT 
						WHERE FILE_ID = V_FILE_ID 
						AND PARENTPRODUCTSPECIFICATIONID = J.PRODUCTSPECIFICATIONID
                        AND PRODUCTSPECIFICATIONID NOT IN ('S0349018')
						)
				LOOP 
                    PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| K.PRODUCTSPECIFICATIONID, SYSDATE );


					/*SELECT COUNT(1)
					INTO V_NY_PT_COUNT2
					FROM NY_MIG_PRODUCT_TREE PT,
                        ny_mig_product_link LP
                    WHERE PT.ordernumber = LP.ordernumber
                    AND PT.S_CODE = LP.S_CODE
                    AND PT.parent_S_CODE = LP.parent_S_CODE
					AND EXISTS (SELECT 
									1 
								FROM 
									NY_MIG_TMF_ORDERPRODUCT TM
								WHERE TM.PRODUCTSPECIFICATIONID = PT.S_CODE
								AND TM.ORDERNUMBER = V_ORDERNUMBER
								AND PRODUCTSPECIFICATIONID = K.PRODUCTSPECIFICATIONID
                                AND TM.PARENTPRODUCTSPECIFICATIONID = LP.LINK_PRODUCT_SCODE
								)
					AND PT.ORDERNUMBER = V_ORDERNUMBER	
					AND PT.S_CODE = K.PRODUCTSPECIFICATIONID
                    AND LP.LINK_PRODUCT_SCODE  = K.PARENTPRODUCTSPECIFICATIONID
					AND PT.STATUS = 'NEW';	
                    */

                    SELECT COUNT(1)
					INTO V_NY_PT_COUNT2
					FROM NY_MIG_PRODUCT_TREE PT
                    WHERE EXISTS (SELECT 
									1 
								FROM 
									NY_MIG_TMF_ORDERPRODUCT TM
								WHERE TM.PRODUCTSPECIFICATIONID = PT.S_CODE
								AND TM.FILE_ID = V_FILE_ID 
								AND PRODUCTSPECIFICATIONID = K.PRODUCTSPECIFICATIONID
								)
					AND PT.FILE_ID = V_FILE_ID 	
					AND PT.S_CODE = K.PRODUCTSPECIFICATIONID
					AND PT.STATUS = 'NEW';	

                    PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| K.PRODUCTSPECIFICATIONID|| ' V_NY_PT_COUNT2'|| V_NY_PT_COUNT2, SYSDATE );

                    V_CONT_DISC_TYPE_CNT :=0;

                    /*SELECT COUNT(1) 
                    INTO V_CONT_DISC_TYPE_CNT
                    from driver_asset_mapping DM
                    WHERE DM.S_CODE = K.PRODUCTSPECIFICATIONID
					AND (REGEXP_LIKE(UPPER(DM.PRODUCT_TYPE),V_OFFER_LIST)
                    OR  (REGEXP_LIKE(UPPER(DM.PRODUCT_TYPE),'BBCONTRACT|BBSWCONT|BBCHOPCONT|TVCONTRACT')));*/

                    SELECT COUNT(1) 
                    INTO V_CONT_DISC_TYPE_CNT
                    from NY_MIG_PRODUCT_TREE PT
                    WHERE PT.FILE_ID = V_FILE_ID 
                    AND PT.S_CODE like  K.PRODUCTSPECIFICATIONID||'%'
					AND (REGEXP_LIKE(UPPER(PT.PRODUCT_TYPE),V_OFFER_LIST)
                    OR  (REGEXP_LIKE(UPPER(PT.PRODUCT_TYPE),'BBCONTRACT|BBSWCONT|BBCHOPCONT|TVCONTRACT'))
                      );
 /*                   
                    SELECT count(*)
                    INTO V_BOX_CNT2
                    FROM NY_MIG_TMF_ORDERPRODUCT TMF,
                    NY_MIG_ORDER_ATTRIBUTES OA
                    WHERE OA.ORDERNUMBER = v_ordernumber
                    AND OA.ORDERNUMBER = TMF.ORDERNUMBER
                    AND OA.ATTRIBUTENAME = 'TOGGLE'
                    AND TMF.PRODUCTSPECIFICATIONID IN ('S0507144','S0507145');
*/
                    PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| K.PRODUCTSPECIFICATIONID|| ' V_CONT_DISC_TYPE_CNT'|| V_CONT_DISC_TYPE_CNT, SYSDATE );

					IF V_NY_PT_COUNT2 >0  and V_CONT_DISC_TYPE_CNT = 0 THEN 
					-- Update action

						/*UPDATE 
								NY_MIG_PRODUCT_TREE PT
							SET PT.VLOCITY_CMT__ACTION__C = 'Update' 
						WHERE ORDERNUMBER = V_ORDERNUMBER	
                        AND S_CODE = K.PRODUCTSPECIFICATIONID
                        and exists (select 1 from ny_mig_product_link LP , NY_MIG_TMF_ORDERPRODUCT TP
                                        WHERE PT.ordernumber = LP.ordernumber
                                            AND PT.S_CODE = LP.S_CODE
                                            AND PT.parent_S_CODE = LP.parent_S_CODE
                                            and TP.ORDERNUMBER = TP.ORDERNUMBER 
                                            AND TP.PRODUCTSPECIFICATIONID = LP.S_CODE
                                                    AND TP.PARENTPRODUCTSPECIFICATIONID = LP.LINK_PRODUCT_SCODE);
                      */                              
                      UPDATE 
								NY_MIG_PRODUCT_TREE PT
							SET PT.OV_ACTION = 'Update' , 
                                PT.PARENTPRODUCTSPECIFICATIONID = K.PARENTPRODUCTSPECIFICATIONID 
						WHERE FILE_ID = V_FILE_ID 	
                        AND S_CODE = K.PRODUCTSPECIFICATIONID;
                    /*    
                     ELSIF
                      V_NY_PT_COUNT2 > 0  and V_BOX_CNT2 > 0 THEN

                      V_BASE_ACTION := 'Create';
					UPDATE 
							NY_MIG_PRODUCT_TREE PT
						SET PT.VLOCITY_CMT__ACTION__C = 'Create' 
					WHERE ORDERNUMBER = V_ORDERNUMBER	
					AND S_CODE = J.PRODUCTSPECIFICATIONID;
                    */
					     ELSE 

              /* BRITBOX , APPLETV AND HOME SECURITY changes added by RUDHRA KUMAR :: STARTS */

                            SELECT COUNT(*)
                            INTO V_BRT_CNT9
                            FROM NY_MIG_PRODUCT_TREE
                            WHERE PRODUCT_ID IN ( 'N0001682' ,'N0001651','N0001659','N0001664','N0001662' )
                            AND FILE_ID = V_FILE_ID;      


                  IF 
                    

                      ( K.PRODUCTSPECIFICATIONID  IN ('S0579424','S0579464','S0579504','S0579344','S0579384') and ( V_BRT_CNT9 = 0 OR V_BRT_CNT9 > 0 )) THEN

                            DELETE FROM NY_MIG_PRODUCT_TREE 
                            WHERE S_CODE IN ('S0579424','S0579464','S0579504','S0579344','S0579384')
                            AND FILE_ID = V_FILE_ID;

                             /* BRITBOX , APPLETV AND HOME SECURITY changes added by RUDHRA KUMAR :: ENDS */


                  ELSE 

						-- DELETE 
						INSERT INTO NY_MIG_PRODUCT_TREE (	
								/*    1    */	MAPPING_TYPE,	
								/*    2    */	PRODUCT_TYPE,	
								/*    3    */	PRODUCT_ID,	
								/*    4    */	S_CODE,	
								/*    5    */	NAME,	
								/*    6    */	LINK_TO_SERVICE_POINT,	
								/*    7    */	CONTRACT_TERM,	
								/*    8    */	PARENT_S_CODE,	
								/*    9    */	PARENT,	
								/*    10    */	RULES,	
								/*    11    */	PRODUCT_FAMILY,	
								/*    12    */	ATTRIBUTE_TYPE,	
								/*    13    */	ATTRIBUTE_DISPLAY_NAME,	
								/*    14    */	UNITPRICE,	
								/*    15    */	DVCPE_DEVICE_QUANTITY,	
								/*    16    */	DISCOUNTAMOUNT,	
								/*    17    */	PRODUCTTERM,	
								/*    18    */	PRODUCTTERMUNIT,	
								/*    19    */	FILE_ID,	
								/*    20    */	ORD_PROD_ID,	
								/*    21    */	ORDERNUMBER,	
								/*    22    */	BILLINGACCOUNTNO,	
								/*    23    */	APPOINTMENT_REFERENCE__C,	
								/*    24    */	APPOINTMENTENDDATE,	
								/*    25    */	APPOINTMENTSTATUS,	
								/*    26    */	APPOINTMENTSTARTDATE,	
								/*    27    */	APPOINTMENTCREATEDDATE,	
								/*    28    */	APPOINTMENTPRODUCTTYPE,	
								/*    29    */	VLOCITY_CMT__ONETIMECHARGE__C,	
								/*    30    */	VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,	
								/*    31    */	PRODUCTNAME,	
								/*    32    */	VLOCITY_CMT__LINENUMBER__C,	
								/*    33    */	VLOCITY_CMT__FULFILMENTSTATUS__C,	
								/*    34    */	VLOCITY_CMT__ISPONRREACHED__C,	
								/*    35    */	VLOCITY_CMT__PROVISIONINGSTATUS__C,	
								/*    36    */	PRODUCTCODE,	
								/*    37    */	PRODUCTID,	
								/*    38    */	NG_CUSTOMERREQUESTEDDATE__C,	
								/*    39    */	VLOCITY_CMT__RECURRINGCHARGE__C,	
								/*    40    */	QUANTITY,	
								/*    41    */	VLOCITY_CMT__RECURRINGTOTAL__C,	
								/*    42    */	ORDERITEMNUMBER,	
								/*    43    */	VLOCITY_CMT__ACTION__C,	
								/*    44    */	COM_CALC_RECURRINGCHARGE__C,	
								/*    45    */	DELIVERY_INSTRUCTIONS__C,	
								/*    46    */	STATUS ,
								/*    47    */  OV_ACTION,
                                /*    48    */  VLOCITY_CMT__SUBACTION__C, 
								/*    49    */ PARENTPRODUCTSPECIFICATIONID
								)
						SELECT 
							/*    1    */ CASE WHEN UPPER(K.PRODUCTRELATIONSHIPTYPE) = 'CHILD'  THEN 'child' ELSE 'root' END as MAPPING_TYPE, 
							/*    2    */ '' AS PRODUCT_TYPE,--K.PRODUCTSPECIFICATIONPRODUCTTYPE AS PRODUCT_TYPE,  
							/*    3    */ '' AS PRODUCT_ID,--K.PRODUCTIDENTIFIER AS PRODUCT_ID, 
							/*    4    */ K.PRODUCTSPECIFICATIONID,
							/*    5    */ K.PRODUCTSPECIFICATIONNAME name, 
							/*    6    */ 'N' AS LINK_TO_SERVICE_POINT,
							/*    7    */ '' AS CONTRACT_TERM, 
							/*    8    */ CASE WHEN UPPER(K.PRODUCTRELATIONSHIPTYPE) = 'CHILD'  THEN J.PRODUCTSPECIFICATIONID ELSE K.PRODUCTSPECIFICATIONID END ,
							/*    9    */ CASE WHEN UPPER(K.PRODUCTRELATIONSHIPTYPE) = 'CHILD'  THEN J.PRODUCTSPECIFICATIONNAME ELSE K.PRODUCTSPECIFICATIONNAME  END , 
							/*    10    */ '1' AS RULES, 
							/*    11    */ '' AS PRODUCT_FAMILY, -- K.PRODUCTSPECIFICATIONFAMILY as PRODUCT_FAMILY, 
							/*    12    */ '' AS ATTRIBUTE_TYPE, 
							/*    13    */ '' AS ATTRIBUTE_DISPLAY_NAME, 
							/*    14    */ '' AS  unitPrice,
							/*    15    */ '' DVCPE_DEVICE_QUANTITY, 
							/*    16    */ '' DISCOUNTAMOUNT, 
							/*    17    */ '' PRODUCTTERM, 
							/*    18    */ '' PRODUCTTERMUNIT, 
							/*    19    */ K.FILE_ID, 
							/*    20    */ K.ORDER_ID, 
							/*    21    */ K.ORDERNUMBER, 
							/*    22    */ K.BILLINGACCOUNTNO, 
							/*    23    */ '' APPOINTMENT_REFERENCE__C,--OP.APPOINTMENT_REFERENCE__C, 
							/*    24    */ '' APPOINTMENTENDDATE, -- OP.APPOINTMENTENDDATE, 
							/*    25    */ '' APPOINTMENTSTATUS,--OP.APPOINTMENTSTATUS, 
							/*    26    */ '' APPOINTMENTSTARTDATE,--OP.APPOINTMENTSTARTDATE, 
							/*    27    */ '' APPOINTMENTCREATEDDATE,--OP.APPOINTMENTCREATEDDATE, 
							/*    28    */ '' APPOINTMENTPRODUCTTYPE,--OP.APPOINTMENTPRODUCTTYPE, 
							/*    29    */ '' VLOCITY_CMT__ONETIMECHARGE__C,--OP.VLOCITY_CMT__ONETIMECHARGE__C, 
							/*    30    */ '' VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,--OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C, 
							/*    31    */ K.NAME, --OP.PRODUCTNAME, 
							/*    32    */ '' VLOCITY_CMT__LINENUMBER__C,--OP.VLOCITY_CMT__LINENUMBER__C, 
							/*    33    */ '' VLOCITY_CMT__FULFILMENTSTATUS__C,--OP.VLOCITY_CMT__FULFILMENTSTATUS__C, 
							/*    34    */ '' VLOCITY_CMT__ISPONRREACHED__C,--OP.VLOCITY_CMT__ISPONRREACHED__C, 
							/*    35    */ '' VLOCITY_CMT__PROVISIONINGSTATUS__C,--OP.VLOCITY_CMT__PROVISIONINGSTATUS__C, 
							/*    36    */ '' PRODUCTCODE,--OP.PRODUCTCODE, 
							/*    37    */ K.PRODUCTID, 
							/*    38    */ '' NG_CUSTOMERREQUESTEDDATE__C,--OP.NG_CUSTOMERREQUESTEDDATE__C, 
							/*    39    */ '' VLOCITY_CMT__RECURRINGCHARGE__C,--OP.VLOCITY_CMT__RECURRINGCHARGE__C, 
							/*    40    */ '' QUANTITY,--OP.QUANTITY, 
							/*    41    */ '' VLOCITY_CMT__RECURRINGTOTAL__C,--OP.VLOCITY_CMT__RECURRINGTOTAL__C, 
							/*    42    */ '' ORDERITEMNUMBER,--OP.ORDERITEMNUMBER, 
							/*    43    */ 'Delete' as  VLOCITY_CMT__ACTION__C,--OP.VLOCITY_CMT__ACTION__C, 
							/*    44    */ '' COM_CALC_RECURRINGCHARGE__C,--OP.COM_CALC_RECURRINGCHARGE__C, 
							/*    45    */ '' DELIVERY_INSTRUCTIONS__C,--OP.DELIVERY_INSTRUCTIONS__C, 
							/*    46    */ 'NEW',
						    /*    47    */  'Delete',
                            /*    47    */ '' VLOCITY_CMT__SUBACTION__C, 
							/*    49    */ K.PARENTPRODUCTSPECIFICATIONID
							from 
                            DUAL;
							--NY_MIG_TMF_ORDERPRODUCT OP
							--WHERE 
							--OP.ORDERNUMBER = V_ORDERNUMBER
							--AND PRODUCTSPECIFICATIONID = K.PRODUCTSPECIFICATIONID
							--AND OP.CONSIDER_FOR_REGRADE = 'Y'
							--;
                      END IF;
					END IF;
			FOR L IN (SELECT * FROM NY_MIG_TMF_ORDERPRODUCT 
						WHERE FILE_ID = V_FILE_ID 
						AND PARENTPRODUCTSPECIFICATIONID = K.PRODUCTSPECIFICATIONID
                        AND PRODUCTSPECIFICATIONID NOT IN ('S0349018')
						)                                                     /* Added for 4th level */
				LOOP 
                    PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| L.PRODUCTSPECIFICATIONID, SYSDATE );


                    SELECT COUNT(1)
					INTO V_NY_PT_COUNT3
					FROM NY_MIG_PRODUCT_TREE PT
                                        WHERE EXISTS (SELECT 
									1 
								FROM 
									NY_MIG_TMF_ORDERPRODUCT TM
								WHERE TM.PRODUCTSPECIFICATIONID = PT.S_CODE
								AND TM.FILE_ID = V_FILE_ID 
								AND PRODUCTSPECIFICATIONID = L.PRODUCTSPECIFICATIONID
								)
					AND PT.FILE_ID = V_FILE_ID 	
					AND PT.S_CODE = L.PRODUCTSPECIFICATIONID
                    AND UPPER(PT.OV_ACTION) <> 'DELETE' 
					AND PT.STATUS = 'NEW';	

                    PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| L.PRODUCTSPECIFICATIONID|| ' V_NY_PT_COUNT3'|| V_NY_PT_COUNT3, SYSDATE );

                    V_CONT_DISC_TYPE_CNT :=0;

                    SELECT COUNT(1) 
                    INTO V_CONT_DISC_TYPE_CNT
                    from NY_MIG_PRODUCT_TREE PT
                    WHERE PT.FILE_ID = V_FILE_ID 
                    AND PT.S_CODE like  L.PRODUCTSPECIFICATIONID||'%'
					AND (REGEXP_LIKE(UPPER(PT.PRODUCT_TYPE),V_OFFER_LIST)
                    OR  (REGEXP_LIKE(UPPER(PT.PRODUCT_TYPE),'BBCONTRACT|BBSWCONT|BBCHOPCONT|TVCONTRACT'))
                     );                    

                    PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'LOAD the ORDER_DETAILS from TMF JSON to Source file','PRC_TMF_UPDATE_PRODUCT_TREE','Order product loaded successfully','NEW', 'Order Number => ' || V_ORDERNUMBER ||' S Code '|| L.PRODUCTSPECIFICATIONID|| ' V_CONT_DISC_TYPE_CNT'|| V_CONT_DISC_TYPE_CNT, SYSDATE );

					IF V_NY_PT_COUNT3 >0  and V_CONT_DISC_TYPE_CNT = 0 THEN 
					-- Update action


                                                        UPDATE NY_MIG_PRODUCT_TREE PT
							                             SET PT.OV_ACTION = 'Update' , 
                                                            PT.PARENTPRODUCTSPECIFICATIONID = L.PARENTPRODUCTSPECIFICATIONID 
						                                  WHERE FILE_ID = V_FILE_ID 	
                                                        AND S_CODE = L.PRODUCTSPECIFICATIONID;
					ELSE 
						-- DELETE 
						INSERT INTO NY_MIG_PRODUCT_TREE (	
								/*    1    */	MAPPING_TYPE,	
								/*    2    */	PRODUCT_TYPE,	
								/*    3    */	PRODUCT_ID,	
								/*    4    */	S_CODE,	
								/*    5    */	NAME,	
								/*    6    */	LINK_TO_SERVICE_POINT,	
								/*    7    */	CONTRACT_TERM,	
								/*    8    */	PARENT_S_CODE,	
								/*    9    */	PARENT,	
								/*    10    */	RULES,	
								/*    11    */	PRODUCT_FAMILY,	
								/*    12    */	ATTRIBUTE_TYPE,	
								/*    13    */	ATTRIBUTE_DISPLAY_NAME,	
								/*    14    */	UNITPRICE,	
								/*    15    */	DVCPE_DEVICE_QUANTITY,	
								/*    16    */	DISCOUNTAMOUNT,	
								/*    17    */	PRODUCTTERM,	
								/*    18    */	PRODUCTTERMUNIT,	
								/*    19    */	FILE_ID,	
								/*    20    */	ORD_PROD_ID,	
								/*    21    */	ORDERNUMBER,	
								/*    22    */	BILLINGACCOUNTNO,	
								/*    23    */	APPOINTMENT_REFERENCE__C,	
								/*    24    */	APPOINTMENTENDDATE,	
								/*    25    */	APPOINTMENTSTATUS,	
								/*    26    */	APPOINTMENTSTARTDATE,	
								/*    27    */	APPOINTMENTCREATEDDATE,	
								/*    28    */	APPOINTMENTPRODUCTTYPE,	
								/*    29    */	VLOCITY_CMT__ONETIMECHARGE__C,	
								/*    30    */	VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,	
								/*    31    */	PRODUCTNAME,	
								/*    32    */	VLOCITY_CMT__LINENUMBER__C,	
								/*    33    */	VLOCITY_CMT__FULFILMENTSTATUS__C,	
								/*    34    */	VLOCITY_CMT__ISPONRREACHED__C,	
								/*    35    */	VLOCITY_CMT__PROVISIONINGSTATUS__C,	
								/*    36    */	PRODUCTCODE,	
								/*    37    */	PRODUCTID,	
								/*    38    */	NG_CUSTOMERREQUESTEDDATE__C,	
								/*    39    */	VLOCITY_CMT__RECURRINGCHARGE__C,	
								/*    40    */	QUANTITY,	
								/*    41    */	VLOCITY_CMT__RECURRINGTOTAL__C,	
								/*    42    */	ORDERITEMNUMBER,	
								/*    43    */	VLOCITY_CMT__ACTION__C,	
								/*    44    */	COM_CALC_RECURRINGCHARGE__C,	
								/*    45    */	DELIVERY_INSTRUCTIONS__C,	
                                                                /*    46    */	STATUS ,
                                                                /*    47    */  OV_ACTION,
                                                                /*    48    */  VLOCITY_CMT__SUBACTION__C, 
							/*    49    */ PARENTPRODUCTSPECIFICATIONID
								)
						SELECT 
							/*    1    */ CASE WHEN UPPER(L.PRODUCTRELATIONSHIPTYPE) = 'CHILD'  THEN 'child' ELSE 'root' END as MAPPING_TYPE, 
							/*    2    */ '' AS PRODUCT_TYPE,--L.PRODUCTSPECIFICATIONPRODUCTTYPE AS PRODUCT_TYPE,  
							/*    3    */ '' AS PRODUCT_ID,--L.PRODUCTIDENTIFIER AS PRODUCT_ID, 
							/*    4    */ L.PRODUCTSPECIFICATIONID,
							/*    5    */ L.PRODUCTSPECIFICATIONNAME name, 
							/*    6    */ 'N' AS LINK_TO_SERVICE_POINT,
							/*    7    */ '' AS CONTRACT_TERM, 
							/*    8    */ CASE WHEN UPPER(L.PRODUCTRELATIONSHIPTYPE) = 'CHILD'  THEN K.PRODUCTSPECIFICATIONID ELSE L.PRODUCTSPECIFICATIONID END ,
							/*    9    */ CASE WHEN UPPER(L.PRODUCTRELATIONSHIPTYPE) = 'CHILD'  THEN K.PRODUCTSPECIFICATIONNAME ELSE L.PRODUCTSPECIFICATIONNAME  END , 
							/*    10    */ '1' AS RULES, 
							/*    11    */ '' AS PRODUCT_FAMILY, -- L.PRODUCTSPECIFICATIONFAMILY as PRODUCT_FAMILY, 
							/*    12    */ '' AS ATTRIBUTE_TYPE, 
							/*    13    */ '' AS ATTRIBUTE_DISPLAY_NAME, 
							/*    14    */ '' AS  unitPrice,
							/*    15    */ '' DVCPE_DEVICE_QUANTITY, 
							/*    16    */ '' DISCOUNTAMOUNT, 
							/*    17    */ '' PRODUCTTERM, 
							/*    18    */ '' PRODUCTTERMUNIT, 
							/*    19    */ L.FILE_ID, 
							/*    20    */ L.ORDER_ID, 
							/*    21    */ L.ORDERNUMBER, 
							/*    22    */ L.BILLINGACCOUNTNO, 
							/*    23    */ '' APPOINTMENT_REFERENCE__C,--OP.APPOINTMENT_REFERENCE__C, 
							/*    24    */ '' APPOINTMENTENDDATE, -- OP.APPOINTMENTENDDATE, 
							/*    25    */ '' APPOINTMENTSTATUS,--OP.APPOINTMENTSTATUS, 
							/*    26    */ '' APPOINTMENTSTARTDATE,--OP.APPOINTMENTSTARTDATE, 
							/*    27    */ '' APPOINTMENTCREATEDDATE,--OP.APPOINTMENTCREATEDDATE, 
							/*    28    */ '' APPOINTMENTPRODUCTTYPE,--OP.APPOINTMENTPRODUCTTYPE, 
							/*    29    */ '' VLOCITY_CMT__ONETIMECHARGE__C,--OP.VLOCITY_CMT__ONETIMECHARGE__C, 
							/*    30    */ '' VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C,--OP.VLOCITY_CMT__EFFECTIVEONETIMETOTAL__C, 
							/*    31    */ L.NAME, --OP.PRODUCTNAME, 
							/*    32    */ '' VLOCITY_CMT__LINENUMBER__C,--OP.VLOCITY_CMT__LINENUMBER__C, 
							/*    33    */ '' VLOCITY_CMT__FULFILMENTSTATUS__C,--OP.VLOCITY_CMT__FULFILMENTSTATUS__C, 
							/*    34    */ '' VLOCITY_CMT__ISPONRREACHED__C,--OP.VLOCITY_CMT__ISPONRREACHED__C, 
							/*    35    */ '' VLOCITY_CMT__PROVISIONINGSTATUS__C,--OP.VLOCITY_CMT__PROVISIONINGSTATUS__C, 
							/*    36    */ '' PRODUCTCODE,--OP.PRODUCTCODE, 
							/*    37    */ L.PRODUCTID, 
							/*    38    */ '' NG_CUSTOMERREQUESTEDDATE__C,--OP.NG_CUSTOMERREQUESTEDDATE__C, 
							/*    39    */ '' VLOCITY_CMT__RECURRINGCHARGE__C,--OP.VLOCITY_CMT__RECURRINGCHARGE__C, 
							/*    40    */ '' QUANTITY,--OP.QUANTITY, 
							/*    41    */ '' VLOCITY_CMT__RECURRINGTOTAL__C,--OP.VLOCITY_CMT__RECURRINGTOTAL__C, 
							/*    42    */ '' ORDERITEMNUMBER,--OP.ORDERITEMNUMBER, 
							/*    43    */ 'Delete' as  VLOCITY_CMT__ACTION__C,--OP.VLOCITY_CMT__ACTION__C, 
							/*    44    */ '' COM_CALC_RECURRINGCHARGE__C,--OP.COM_CALC_RECURRINGCHARGE__C, 
							/*    45    */ '' DELIVERY_INSTRUCTIONS__C,--OP.DELIVERY_INSTRUCTIONS__C, 
                            /*    46    */ 'NEW',
                            /*    47    */  'Delete',
                            /*    47    */ '' VLOCITY_CMT__SUBACTION__C, 
							/*    49    */ L.PARENTPRODUCTSPECIFICATIONID
							from 
                            DUAL;
							--NY_MIG_TMF_ORDERPRODUCT OP
							--WHERE 
							--OP.ORDERNUMBER = V_ORDERNUMBER
							--AND PRODUCTSPECIFICATIONID = L.PRODUCTSPECIFICATIONID
							--AND OP.CONSIDER_FOR_REGRADE = 'Y'
							--;

					END IF;

					END LOOP;  -- END OF L loop 

				END LOOP; -- END OF K loop 

			END LOOP; -- END of J loop 

		END LOOP; -- End of I loop 

     		ELSE 
			PKG_NY_MIG_EXCEPTION_LOG.PRC_WORKFLOW_LOG(v_file_id,v_ordernumber,'Update PRODUCT TREE table for regrade','PRC_TMF_UPDATE_PRODUCT_TREE','Order product doesnot exists','NEW', 'Order Number => ' || V_ORDERNUMBER, SYSDATE );

		END IF;
	END IF;

END PRC_TMF_UPDATE_PRODUCT_TREE;

/* PRC_XML_INSTANCECHARACTERSTICLIST is added for None and Amend scenario for attribute level by Bala/Kanagavel/Ranganath on 08-08-2022 */

 procedure PRC_XML_INSTANCECHARACTERSTICLIST(v_ordernumber IN VARCHAR2, v_file_id IN NUMBER,V_IS_REGRADE IN VARCHAR2,V_REGRADE_TYPE IN VARCHAR2)
		AS
    v_t_query                         VARCHAR2(4000); 
    v_productid               ny_mig_product_tree.product_id%TYPE;
    v_parent_s_code           ny_mig_product_tree.parent_s_code%TYPE;
    v_servicelinetype         ny_mig_bb_slt_attributes.servicelinetype%TYPE;
    v_sltvalue                VARCHAR2(500);
    v_action_value            VARCHAR2(30);
    V_ERR_LOG_MSG             VARCHAR2(1000);
    v_prev_sltvalue			  VARCHAR2(500); 
    v_download_speed          VARCHAR2(100);
    v_upload_speed            VARCHAR2(100);
    v_technology              VARCHAR2(100);
    v_ord_attr_cnt            NUMBER := 0;
    IS_NULL                   EXCEPTION; 
    V_TV_DISCOUNT_LIST 		  VARCHAR2(1000);
    V_TMF_OFFERREF            VARCHAR2(500); 
    v_supp_servId             VARCHAR2(500);

BEGIN

	SELECT PAR_NAME INTO V_TV_DISCOUNT_LIST
    FROM NAYAN_CONFIG_PARAMS
    WHERE  PAR_TYPE = 'TV_DISCOUNT_OFFER_LIST';

		FOR c IN (                  
            select  ROWID,
				p.* from
				NY_MIG_PRODUCT_TREE p
			 where
				p.file_id = v_file_id) 
		LOOP

        IF REGEXP_LIKE(UPPER(C.PRODUCT_TYPE),V_TV_DISCOUNT_LIST) AND UPPER(c.OV_ACTION) <> 'DELETE' THEN 

			  IF UPPER(c.OV_ACTION) = 'AMEND' THEN
				BEGIN 

					select PRODUCTCHARACTERISTICVALUE
					INTO V_TMF_OFFERREF
					FROM NY_MIG_TMF_PRODUCTCHARACTERISTIC PC
					WHERE  PC.FILE_ID = V_FILE_ID 
					AND PC.PRODUCTSPECIFICATIONID =  c.s_code
					AND upper(PC.PRODUCTCHARACTERISTICNAME) = upper('offerRef');
				EXCEPTION
				WHEN NO_DATA_FOUND THEN 
					V_TMF_OFFERREF := null;
				END;
			END IF; 

			IF V_TMF_OFFERREF IS NOT NULL THEN 
				v_sltvalue := V_TMF_OFFERREF;
			ELSE    
				v_sltvalue := '';
			END IF; 
            -- insert 
                INSERT INTO NY_MIG_INSTANCECHARACTERSTICLIST 
                (MAPPING_TYPE,
                FILE_ID,
                ORDERNUMBER,
                PRODUCT_TYPE,
                S_CODE,
                S_CODE_NAME,
                PARENT_S_CODE,
                PARENT_NAME,
                ATTRIBUTE_NAME,
                ATTRIBUTE_VALUE,
                ATTRIBUTE_PREVIOUS_VALUE,
                ACTION,
                PROD_TREE_ROWID)
                VALUES(c.MAPPING_TYPE,
                v_file_id,
                v_ordernumber,
                c.PRODUCT_TYPE,
                c.S_CODE,
                c.NAME,
                c.S_CODE,
                c.NAME,
               'offerRef',
               v_sltvalue,
               V_TMF_OFFERREF,
               (case 
                WHEN UPPER(v_sltvalue)= UPPER(V_TMF_OFFERREF) AND upper(c.OV_ACTION) = upper('UPDATE') THEN '-'
                WHEN v_sltvalue is null and V_TMF_OFFERREF is null AND upper(c.OV_ACTION) = upper('UPDATE') THEN '-'
                ELSE 
                c.OV_ACTION END) ,
                c.rowid);            

        END IF;
   IF Upper(c.product_type) = 'BB' and upper(c.mapping_type) = 'ROOT' Then 
												SELECT COUNT(*)
												INTO v_ord_attr_cnt
												FROM ny_mig_order_attributes
												WHERE FILE_ID = V_FILE_ID 
												AND attributename IN ( 'VEPC_ATTR_DT_SUPPLIER_DOWNLOAD_SPEED','VEPC_ATTR_DT_SUPPLIER_UPLOAD_SPEED','VEPC_ATTR_ACCESS_TECHNOLOGY')
                                                AND productcode in (SELECT product_id from driver_asset_mapping Where upper(product_type) = upper('BBBase'));

											pkg_ny_mig_exception_log.prc_workflow_log(v_file_id,
																					 v_ordernumber,
																					 'XML GENERATION',
																					 'PRC_XML_GENERATION',
																					 'orderItem - array attribututes count',
																					 'NEW',
																					 'Order attribute count '|| v_ord_attr_cnt,
																					 sysdate
											);

													IF v_ord_attr_cnt > 0 Then			
														Begin
															SELECT 
																TRIM(REPLACE(attributevalue,'Mbit/s',''))
															INTO v_download_speed
															FROM
																ny_mig_order_attributes
															WHERE
																FILE_ID = V_FILE_ID 
																AND attributename = 'VEPC_ATTR_DT_SUPPLIER_DOWNLOAD_SPEED'
                                                                AND productcode in (SELECT product_id from driver_asset_mapping Where upper(product_type) = upper('BBBase'));

															SELECT
																CASE WHEN upper(attributevalue) = 'N/A' THEN attributevalue ELSE TRIM(REPLACE(attributevalue,'Mbit/s','')) END
															INTO v_upload_speed
															FROM
																ny_mig_order_attributes
															WHERE
																FILE_ID = V_FILE_ID 
																AND attributename = 'VEPC_ATTR_DT_SUPPLIER_UPLOAD_SPEED'
                                                                AND productcode in (SELECT product_id from driver_asset_mapping Where upper(product_type) = upper('BBBase'));

															SELECT
																attributevalue
															INTO v_technology
															FROM
																ny_mig_order_attributes
															WHERE
																FILE_ID = V_FILE_ID 
																AND attributename = 'VEPC_ATTR_ACCESS_TECHNOLOGY'
                                                                AND productcode in (SELECT product_id from driver_asset_mapping Where upper(product_type) = upper('BBBase'));

                                                            IF replace(v_download_speed,' ','') is null or replace(v_upload_speed,' ','') is null or replace(v_technology,' ','') is null THEN
                                                                RAISE IS_NULL;
                                                            END IF; /* Added By Elen */

														Exception
														WHEN NO_DATA_FOUND THEN
                                                        V_ERR_LOG_MSG := 'Order Item xml Tag - ny_mig_order_attributes - Download Speed or upload speed or Technology is missing';
														PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_GENERATION', SYSDATE, 'INTERNAL_ERROR_LOG', V_ERR_LOG_MSG); /* Added By Elen */
                                                        WHEN IS_NULL THEN
                                                        V_ERR_LOG_MSG := 'Order Item xml Tag - ny_mig_order_attributes - Download Speed or upload speed or Technology is Null';
														PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_GENERATION', SYSDATE, 'INTERNAL_ERROR_LOG', V_ERR_LOG_MSG); /* Added By Elen */
														End;

														pkg_ny_mig_exception_log.prc_workflow_log(v_file_id,
																								 v_ordernumber,
																								 'XML GENERATION',
																								 'PRC_XML_GENERATION',
																								 'orderItem - Attributes',
																								 'NEW',
																								 'Download Speed '
																								 || v_download_speed
																								 || ' upload speed '
																								 || v_upload_speed
																								 || ' Technology '
																								 || v_technology,
																								 sysdate
														);
                                                    /* Added By Elen Download Speed or upload speed or Technology attribute is missing - Starts*/
                                                    ELSIF v_ord_attr_cnt = 0 Then
                                                        BEGIN
                                                            IF replace(v_download_speed,' ','') is null or replace(v_upload_speed,' ','') is null or replace(v_technology,' ','') is null THEN
                                                                RAISE IS_NULL;
                                                            END IF; 

														Exception
                                                        WHEN IS_NULL THEN
                                                        V_ERR_LOG_MSG := 'Order Item xml Tag - ny_mig_order_attributes - Download Speed or upload speed or Technology is Null';
														PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_GENERATION', SYSDATE, 'INTERNAL_ERROR_LOG', V_ERR_LOG_MSG);
														End;
                                                    /* Added By Elen Download Speed or upload speed or Technology attribute is missing - ends*/
													END IF;

                                                    BEGIN
                                                        IF v_download_speed IS NOT NULL AND v_upload_speed IS NOT NULL AND v_technology IS NOT NULL THEN
                                                                SELECT SERVICELINETYPE
                                                                INTO v_servicelinetype
                                                                  FROM ny_mig_bb_slt_attributes slt
                                                                 WHERE
                                                                    Trim(Replace(downloadspeed,'Mbps','')) = v_download_speed
                                                                    AND Trim(Replace(UPLOADSPEED,'Mbps','')) = v_upload_speed
                                                                       AND upper(technology) = upper(v_technology);
                                                        END IF;

                                                    Exception
                                                    WHEN OTHERS THEN
                                                    V_ERR_LOG_MSG := 'Order Item xml Tag - ny_mig_bb_slt_attributes - servicelinetype record not exists';
                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_GENERATION', SYSDATE, 'INTERNAL_ERROR_LOG', V_ERR_LOG_MSG); /* Added By Elen */
                                                    END;


														pkg_ny_mig_exception_log.prc_workflow_log(v_file_id,
																								 v_ordernumber,
																								 'XML GENERATION',
																								 'PRC_XML_GENERATION',
																								 'orderItem - Attributes',
																								 'NEW',
																								 'servicelinetype '
																								 || v_servicelinetype,
																								 sysdate
														);
                               End If;

		if upper(c.mapping_type) = 'ROOT' THEN
				FOR j IN ( SELECT AM.MAPPING_TYPE, AM.DRIVER_S_CODE, AM.DRIVER_S_CODE_NAME, AM.NAME, AM.VALUE, AM.PARENT_S_CODE, AM.PARENT, A.*, 
											--(CASE WHEN UPPER(c.VLOCITY_CMT__ACTION__C) = 'ADD' THEN NULL ELSE PC.PRODUCTCHARACTERISTICVALUE  END) as previousvalue
                                            -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
                                            (CASE WHEN UPPER(c.OV_ACTION) = 'ADD' THEN NULL ELSE PC.PRODUCTCHARACTERISTICVALUE  END) as previousvalue
												FROM DRIVER_ASSET_MANDATORY AM, NY_ATTRIBUTE_MAPPING_CONFIG A, NY_MIG_TMF_PRODUCTCHARACTERISTIC PC
											WHERE  PC.ORDERNUMBER(+) = V_ORDERNUMBER
												AND PC.PRODUCTSPECIFICATIONID(+) =  c.s_code
												AND upper(PC.PRODUCTCHARACTERISTICNAME(+)) = upper(AM.NAME)
                                                AND PC.PARENTPRODUCTSPECIFICATIONID(+) = C.PARENTPRODUCTSPECIFICATIONID 
												AND UPPER(AM.VALUE) = UPPER(A.ATTRIBUTE_NAME)
												AND UPPER(AM.MAPPING_TYPE) = 'ATTRIBUTE'
												AND AM.driver_s_code =  c.s_code
                                                -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
                                                --AND UPPER(c.VLOCITY_CMT__ACTION__C) <> 'DELETE' 
                                                AND UPPER(c.OV_ACTION) <> 'DELETE' 
                                            union all  -- Regrade changes for attribute start
                                             SELECT 'attribute' as MAPPING_TYPE, c.s_code AS DRIVER_S_CODE, c.NAME AS DRIVER_S_CODE_NAME, 
                                                PRODUCTCHARACTERISTICNAME as NAME, PRODUCTCHARACTERISTICVALUE as VALUE, 
                                                C.PARENT_S_CODE as PARENT_S_CODE, c.PARENT as PARENT, PRODUCTCHARACTERISTICNAME as ATTRIBUTE_NAME , 
                                                  PRODUCTCHARACTERISTICNAME as ATTRIBUTE_DISPLAY_NAME, null as TABLE_NAME, null as TABLE_COLUMN, null as BUSINESS_LOGIC, 
												PRODUCTCHARACTERISTICVALUE as DEFAULT_VALUE, NULL as previousvalue
                                                FROM NY_MIG_TMF_PRODUCTCHARACTERISTIC
                                                WHERE FILE_ID = V_FILE_ID 
                                                AND PRODUCTSPECIFICATIONID =  c.s_code
                                                -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
                                                --AND UPPER(c.VLOCITY_CMT__ACTION__C) = 'DELETE' 
                                                AND UPPER(c.OV_ACTION) = 'DELETE' 
                                                AND PARENTPRODUCTSPECIFICATIONID = C.PARENTPRODUCTSPECIFICATIONID 
                                                -- Regrade changes for attribute  end
										) 
										LOOP																	

									pkg_ny_mig_exception_log.prc_workflow_log(v_file_id,
																			v_ordernumber,
																			'CONFIGURE NY_MIG_INSTANCECHARACTERSTICLIST',
																			'PRC_XML_INSTANCECHARACTERSTICLIST',
																			'orderItem - Attributes',
																			'NEW',
																			'Root Attribute '||j.name,
																			sysdate
									);
                                                               -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
                                                               -- IF c.VLOCITY_CMT__ACTION__C IS NOT NULL THEN
                                                                  IF c.OV_ACTION IS NOT NULL THEN
                                                                    --v_action_value := (CASE WHEN UPPER(c.VLOCITY_CMT__ACTION__C) = 'ADD' THEN 'Create' END);
                                                                -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
                                                                -- v_action_value := c.VLOCITY_CMT__ACTION__C;
                                                                   v_action_value := c.OV_ACTION;
                                                                END IF;

        --Code added for [SFI-38441]: Regrade -Set BBServiceID For BT Vision Service--Start(Gourav) 
                                                            IF UPPER(j.ATTRIBUTE_DISPLAY_NAME) = 'BBSERVICEID' AND c.S_CODE = 'S0146404' THEN
                        
                                                                BEGIN
                                                                    IF UPPER(c.ov_action) = 'UPDATE'  THEN

                                                                        select PRODUCTINSTANCEVALUE into v_supp_servId from NY_MIG_TMF_PRODUCTINSTANCEIDS
                                                                            where ordernumber = v_ordernumber
                                                                            and PRODUCTSPECIFICATIONID = 'S0145868'
                                                                            and upper(PRODUCTINSTANCENAME) = 'SUPPLIERSERVICEID';
                       
                                                                        v_sltvalue := '' || v_supp_servId  || '';
                                                    
                                                                    END IF;
    
                                                                EXCEPTION
                                                                    WHEN OTHERS THEN
                                                                    v_sltvalue := '';
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                END;
																
        --Code added for [SFI-38441]: Regrade -Set BBServiceID For BT Vision Service--End(Gourav)
        
																ELSIF j.TABLE_NAME = 'NY_MIG_BB_SLT_ATTRIBUTES'	THEN

                                                                 If v_servicelinetype is NOT NULL Then
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;

																	EXECUTE IMMEDIATE 'SELECT '
																					  || j.TABLE_COLUMN
																					  || ' FROM '
																					  || j.TABLE_NAME 
																					  || ' WHERE SERVICELINETYPE = '''
																					  || v_servicelinetype
																					  || ''''
																	INTO v_sltvalue;
                                                                 Else
                                                                   v_sltvalue := '';
                                                                 End If;

                                                                              -- Regrade Marker change start
                                                                ELSIF j.TABLE_NAME = 'NY_MIG_BILLING_ACCOUNT'	THEN
                                                                    IF upper(j.ATTRIBUTE_NAME) = upper('[PM.targetBillingAccount]') THEN 
                                                                        EXECUTE IMMEDIATE 'SELECT '
                                                                                        || j.TABLE_COLUMN
                                                                                        || ' FROM '
                                                                                        || j.TABLE_NAME 
                                                                                        || ' WHERE FILE_ID = '''
                                                                                        || V_FILE_ID
                                                                                        || ''' AND BRAND = '''
                                                                                        || 'EE'
                                                                                        || ''''

                                                                                        INTO v_sltvalue;

                                                                    ELSIF upper(j.ATTRIBUTE_NAME) = upper('[PM.sourceBillingAccoun]') THEN  
                                                                        EXECUTE IMMEDIATE 'SELECT '
                                                                                        || j.TABLE_COLUMN
                                                                                        || ' FROM '
                                                                                        || j.TABLE_NAME 
                                                                                        || ' WHERE FILE_ID = '''
                                                                                        || V_FILE_ID
                                                                                        || ''' AND BRAND = '''
                                                                                        || 'BT Retail Consumer'
                                                                                        || ''''

                                                                                        INTO v_sltvalue;

                                                                    END IF;
                                                                -- Regrade Marker change End

																ELSIF j.TABLE_NAME = 'NY_MIG_ORDER_ATTRIBUTES'	THEN 

                                                                V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;

                                                                v_sltvalue := '';

                                                                IF upper(j.ATTRIBUTE_DISPLAY_NAME) = 'CONTRACTTERM' Then

                                                                    v_sltvalue := '';

                                                                    BEGIN

                                                                        EXECUTE IMMEDIATE  'SELECT REGEXP_REPLACE(attributevalue,''[^[:digit:]]'','''') FROM ' 
                                                                                            || ' NY_MIG_ORDER_ATTRIBUTES OA ' 
                                                                                            || ' WHERE EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM WHERE AM.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM.PRODUCT_TYPE) IN (''BBBASE'',''TVBASE'',''BBCHOPBASE'',''BBSWBASE'',''SPORTBASE''))' --Added by kanagavel
                                                                                            || ' AND OA.FILE_ID = '''
                                                                                            || V_FILE_ID 
                                                                                            || ''' and OA.PRODUCTCODE = ''' 
                                                                                            || c.PRODUCTCODE                                                                                     
                                                                                            || ''' and OA.attributename = ''' 
                                                                                            || j.TABLE_COLUMN 
                                                                                            || ''''																	
                                                                            INTO v_sltvalue;
                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;

                                                                ELSIF upper(j.ATTRIBUTE_DISPLAY_NAME) = 'RETAINEXISTINGNUMBER' Then
                                                                    BEGIN
                                                                        EXECUTE IMMEDIATE  'SELECT (CASE WHEN UPPER(attributevalue) = ''PORTED'' THEN ''Y'' WHEN UPPER(attributevalue) = ''NEW'' THEN ''N'' END) FROM ' 
                                                                                            || ' NY_MIG_ORDER_ATTRIBUTES OA ' 
                                                                                            || ' WHERE EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM WHERE AM.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM.PRODUCT_TYPE) = UPPER('''
                                                                                            || c.product_type
                                                                                            || '''))'
                                                                                            || ' AND OA.FILE_ID = '''
                                                                                            || V_FILE_ID
                                                                                            || ''' and OA.PRODUCTCODE = ''' 
                                                                                            || c.PRODUCTCODE                                                                                    
                                                                                            || ''' and OA.attributename = ''' 
                                                                                            || j.TABLE_COLUMN 
                                                                                            || ''''																	
                                                                            INTO v_sltvalue;

                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;
                                                                    /* code changes done by sree*/
                                                                     ELSIF upper(j.ATTRIBUTE_NAME) = upper('[PM.PendingCease]') and c.s_code IN ('S0336359','S0331123','S0442063','S0392436','S0443284','S0502644') Then
                                                                    BEGIN
                                                                        EXECUTE IMMEDIATE  'SELECT (CASE WHEN UPPER(attributevalue) = ''PENDING CEASE'' THEN ''Y'' WHEN UPPER(attributevalue) = ''ACTIVE'' THEN ''N'' END) FROM ' 
                                                                                            || ' NY_MIG_ORDER_ATTRIBUTES OA ' 
                                                                                            || ' WHERE EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM WHERE AM.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM.PRODUCT_TYPE) = UPPER('''
                                                                                            || c.product_type
                                                                                            || '''))'
                                                                                            || ' AND OA.FILE_ID = '''
                                                                                            || V_FILE_ID
                                                                                            || ''' and OA.PRODUCTCODE = ''' 
                                                                                            || c.PRODUCTCODE                                                                                    
                                                                                            || ''' and OA.attributename = ''' 
                                                                                            || j.TABLE_COLUMN 
                                                                                            || ''''																	
                                                                            INTO v_sltvalue;

                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;

                                                                 ELSIF upper(j.ATTRIBUTE_NAME) = upper('[PM.activationURL]') and c.s_code='S0443104' Then
                                                                    BEGIN
                                                                        EXECUTE IMMEDIATE  'SELECT ''Add'' FROM ' 
                                                                                            || ' NY_MIG_ORDER_ATTRIBUTES OA ' 
                                                                                            || ' WHERE EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM WHERE AM.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM.PRODUCT_TYPE) = UPPER('''
                                                                                            || c.product_type
                                                                                            || '''))'
                                                                                            || ' AND OA.FILE_ID = '''
                                                                                            || V_FILE_ID
                                                                                            || ''' and OA.PRODUCTCODE = ''' 
                                                                                            || c.PRODUCTCODE 
                                                                                              || ''' and OA.attributvalue = ''' 
                                                                                            || 'Pending Activation' 
                                                                                            || ''''		
                                                                                            || ''' and OA.attributename = ''' 
                                                                                            || j.TABLE_COLUMN 
                                                                                            || ''''																	
                                                                            INTO v_sltvalue;

                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;
                                                                /*code changes end by sree */


/* Code changes ADDED for Asset Mandatory for "nowTVProvisionInstruction" on 08-Apr-22 by Paul :: Starts */
ELSIF upper(j.ATTRIBUTE_DISPLAY_NAME) = upper('nowTVProvisionInstruction') Then /* Update by Paul on 05-Apr-22 */
                                                                    BEGIN
                                                                        EXECUTE IMMEDIATE  'SELECT (CASE WHEN UPPER(attributevalue) = ''NEW'' THEN ''Create Account'' WHEN UPPER(attributevalue) = ''PROCEEDABLE'' THEN ''Link Account'' END) FROM '  /* Update by Paul on 05-Apr-22 */
                                                                                            || ' NY_MIG_ORDER_ATTRIBUTES OA ' 
                                                                                            || ' WHERE EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM WHERE AM.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM.PRODUCT_TYPE) = UPPER('''
                                                                                            || c.product_type
                                                                                            || '''))'
                                                                                            || ' AND OA.FILE_ID = '''
                                                                                            || V_FILE_ID
                                                                                            || ''' and OA.PRODUCTCODE = ''' 
                                                                                            || c.PRODUCTCODE                                                                                    
                                                                                            || ''' and OA.attributename = ''' 
                                                                                            || j.TABLE_COLUMN 
                                                                                            || ''''																	
                                                                            INTO v_sltvalue;

                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;
/* Code changes ADDED for Asset Mandatory for "nowTVProvisionInstruction" on 08-Apr-22 by Paul :: Ends */


/* Code changes ADDED for "C0 - DV directory Entry Type" on 18-May-22 by Paul :: Starts */
ELSIF upper(j.ATTRIBUTE_DISPLAY_NAME) = upper('DirectoryEntryType') Then 
                                                                    BEGIN
                                                                        EXECUTE IMMEDIATE  'SELECT (CASE WHEN UPPER(attributevalue) = ''ORDINARY DIRECTORY ENTRY'' THEN ''O''
																										WHEN UPPER(attributevalue) = ''DIRECTORY ENQUIRIES ONLY'' THEN ''DQR''
																										WHEN UPPER(attributevalue) = ''NO DIRECTORY ENTRY'' THEN ''NQR''
																										WHEN UPPER(attributevalue) = ''EX-DIRECTORY (NO CALLS OFFERED)'' THEN ''XDNC'' END) FROM '
                                                                                            || ' NY_MIG_ORDER_ATTRIBUTES OA ' 
                                                                                            || ' WHERE EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM WHERE AM.PRODUCT_ID = OA.PRODUCTCODE)'
                                                                                            || ' AND OA.FILE_ID = '''
                                                                                            || V_FILE_ID
                                                                                            || ''' and OA.PRODUCTCODE = ''' 
                                                                                            || 'N0001136'                                                                                    
                                                                                            || ''' and OA.attributename = ''' 
                                                                                            || j.TABLE_COLUMN 
                                                                                            || ''''																	
                                                                            INTO v_sltvalue;

                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;
/* Code changes ADDED for "C0 - DV directory Entry Type" on 18-May-22 by Paul :: Ends */

                                                                ELSE 
                                                                    BEGIN
                                                                        EXECUTE IMMEDIATE  'SELECT attributevalue FROM ' 
                                                                                            || ' NY_MIG_ORDER_ATTRIBUTES OA ' 
                                                                                            || ' WHERE EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM WHERE AM.PRODUCT_ID = OA.PRODUCTCODE)'
                                                                                            || ' AND OA.FILE_ID = '''
                                                                                            || V_FILE_ID
                                                                                            || ''' and OA.PRODUCTCODE = ''' 
                                                                                            || c.PRODUCTCODE                                                                                    
                                                                                            || ''' and OA.attributename = ''' 
                                                                                            || j.TABLE_COLUMN 
                                                                                            || ''''																	
                                                                            INTO v_sltvalue;

                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;

                                                                END IF;

																ELSIF j.TABLE_NAME = 'NY_MIG_PRODUCT_LINK' THEN

                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;

																	EXECUTE IMMEDIATE 'SELECT '
																	                  || j.TABLE_COLUMN 
																					  || ' FROM ' 
																					  || j.TABLE_NAME 
																					  || ' WHERE FILE_ID = '''
																					  || V_FILE_ID 
																					  || ''' and PRODUCT_ID = ''' 
																					  || v_productid 
																					  || ''' and name = ''' 
																					  || j.name || ''''
																	INTO v_sltvalue;

                                    /* Code changes ADDED for "AppleTV and BritBox" on 19-May-22 by Paul :: Starts */
                                    ELSIF j.TABLE_NAME = 'NY_MIG_PRODUCT_TREE' THEN
                                                v_sltvalue := '';
                                                IF upper(j.ATTRIBUTE_NAME) = upper('[PM.Productidentifier]') THEN
                                                             IF C.PRODUCTCODE = 'N0001036' THEN
                                                                    BEGIN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;

																	EXECUTE IMMEDIATE 'SELECT CASE WHEN :V_IS_REGRADE1 = ''N''  AND  PT.fulfillmentInstruction IS NULL  AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN = ''TP-6M'' THEN ''S0578325''
																								   WHEN :V_IS_REGRADE2 = ''N''  AND  PT.fulfillmentInstruction IS NULL  AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN = ''TP-9M'' THEN ''S0578324''
																								   WHEN :V_IS_REGRADE3 = ''N''  AND  PT.fulfillmentInstruction IS NULL  AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN = ''TP-12M'' THEN ''S0578304''
																								   WHEN :V_IS_REGRADE4 = ''N''  AND  PT.fulfillmentInstruction IS NULL  AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN IS NULL THEN ''S0573304''
																								   WHEN :V_IS_REGRADE5 = ''Y''  AND  PT.fulfillmentInstruction IS NULL  AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN = ''TP-6M'' THEN ''S0578325''
																								   WHEN :V_IS_REGRADE6 = ''Y''  AND  PT.fulfillmentInstruction IS NULL  AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN = ''TP-9M'' THEN ''S0578324''
																								   WHEN :V_IS_REGRADE7 = ''Y''  AND  PT.fulfillmentInstruction IS NULL  AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN = ''TP-12M'' THEN ''S0578304''
																								   WHEN :V_IS_REGRADE8 = ''Y''  AND  PT.fulfillmentInstruction IS NULL  AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN IS NULL THEN ''S0573304''
																								   WHEN :V_IS_REGRADE9 = ''Y'' AND ((PT.fulfillmentInstruction = ''AutoRenewOn'' AND PT.CEASE_DATE IS NULL) OR (PT.fulfillmentInstruction = ''AutoRenewOff'' AND PT.CEASE_DATE IS NOT NULL)) AND TMF.PRODUCTSPECIFICATIONID IN (''S0578325'',''S0578324'',''S0578304'',''S0573304'') THEN TMF.PRODUCTSPECIFICATIONID
																								   WHEN :V_IS_REGRADE10 = ''Y'' AND PT.fulfillmentInstruction IS NULL  AND TMF.PRODUCTSPECIFICATIONID IN (''S0578325'',''S0578324'',''S0578304'',''S0573304'') THEN NULL
																								END  '               
																									|| ' FROM NY_MIG_PRODUCT_TREE PT, NY_MIG_DISCOUNT_ATTRIBUTES DS ,NY_MIG_TMF_ORDERPRODUCT TMF WHERE PT.FILE_ID='''
																									|| V_FILE_ID
																									||''' AND PT.ORDERNUMBER = DS.ORDERNUMBER(+) AND PT.PRODUCT_ID=DS.PRODUCTCODE(+) AND PT.ORDERNUMBER = TMF.ORDERNUMBER (+) AND tmf.productspecificationid(+) IN (''S0578325'',''S0578324'',''S0578304'',''S0573304'') '
																									|| ' AND PT.ROWID = ''' 
																									||  c.rowid  
																									|| ''''  INTO v_sltvalue USING V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE;


                                                                    Exception   
                                                                    WHEN OTHERS THEN
                                                                    v_sltvalue := '';
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_T_QUERY);
                                                                    END;

                                                                ELSIF C.PRODUCTCODE = 'N0001313' THEN

                                                                     BEGIN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;

																	EXECUTE IMMEDIATE 'SELECT CASE WHEN :V_IS_REGRADE1 = ''N''  AND  PT.fulfillmentInstruction IS NULL AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN IS NOT NULL THEN ''S0507145''
                                                                                                   WHEN :V_IS_REGRADE2 = ''N''  AND  PT.fulfillmentInstruction IS NULL  AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN IS NULL THEN ''S0507144''
                                                                                                   WHEN :V_IS_REGRADE3 = ''N''  AND  PT.fulfillmentInstruction = ''Subscribe'' AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN IS NOT NULL THEN ''S0507145''
                                                                                                   WHEN :V_IS_REGRADE4 = ''N''  AND  PT.fulfillmentInstruction = ''Subscribe'' AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN IS NULL THEN ''S0507144''
                                                                                                   WHEN :V_IS_REGRADE5 = ''Y''  AND  PT.fulfillmentInstruction = ''Subscribe'' AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN IS NOT NULL  THEN ''S0507145''
                                                                                                   WHEN :V_IS_REGRADE6 = ''Y''  AND  PT.fulfillmentInstruction = ''Subscribe'' AND PT.CEASE_DATE IS NULL AND DS.TIMEPLAN IS NULL THEN ''S0507144''
                                                                                                   WHEN :V_IS_REGRADE7 = ''Y'' AND ((PT.fulfillmentInstruction = ''AutoRenewOn'' AND PT.CEASE_DATE IS NULL) OR (PT.fulfillmentInstruction = ''AutoRenewOff'' AND PT.CEASE_DATE IS NOT NULL)) AND TMF.PRODUCTSPECIFICATIONID IN (''S0507144'',''S0507145'') THEN TMF.PRODUCTSPECIFICATIONID
                                                                                                   WHEN :V_IS_REGRADE8 = ''Y'' AND PT.fulfillmentInstruction IS NULL THEN NULL
                                                                                               END '               
                                                                                || ' FROM NY_MIG_PRODUCT_TREE PT, NY_MIG_DISCOUNT_ATTRIBUTES DS ,NY_MIG_TMF_ORDERPRODUCT TMF WHERE PT.FILE_ID='''
                                                                                || V_FILE_ID
                                                                                ||''' AND PT.ORDERNUMBER = DS.ORDERNUMBER(+) AND PT.PRODUCT_ID=DS.PRODUCTCODE(+) AND PT.ORDERNUMBER = TMF.ORDERNUMBER (+) AND tmf.productspecificationid(+) IN ( ''S0507144'', ''S0507145'' ) '
                                                                                || ' AND PT.ROWID = ''' 
                                                                                ||  c.rowid  
                                                                                || ''''  INTO v_sltvalue USING V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE;

                                                                    Exception   
                                                                    WHEN OTHERS THEN
                                                                    v_sltvalue := '';
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_T_QUERY);
                                                                    END;

                                                 /* HOME SECURITY logic Added by RUDHRA KUMAR ON  14-08-22 ::STARTS*/


                                                                  ELSIF C.PRODUCTCODE IN ('N0001682','N0001651','N0001659','N0001664','N0001662') THEN

                                                                        BEGIN
                                                                        V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;

                                                                        EXECUTE IMMEDIATE 'SELECT CASE WHEN :V_IS_REGRADE1 = ''Y''   AND PT.PRODUCTCODE = ''N0001682''  THEN ''S0579424''
                                                                                                       WHEN :V_IS_REGRADE2 = ''Y''   AND PT.PRODUCTCODE = ''N0001651''  THEN ''S0579464''
                                                                                                       WHEN :V_IS_REGRADE3 = ''Y''   AND PT.PRODUCTCODE = ''N0001659''  THEN ''S0579504''
                                                                                                       WHEN :V_IS_REGRADE4 = ''Y''   AND PT.PRODUCTCODE = ''N0001664''  THEN ''S0579344''
                                                                                                       WHEN :V_IS_REGRADE5 = ''Y''   AND PT.PRODUCTCODE = ''N0001662''  THEN ''S0579384''
                                                                                                       WHEN :V_IS_REGRADE6 = ''N''   AND PT.PRODUCTCODE = ''N0001682''  THEN ''S0579424''
                                                                                                       WHEN :V_IS_REGRADE7 = ''N''   AND PT.PRODUCTCODE = ''N0001651''  THEN ''S0579464''
                                                                                                       WHEN :V_IS_REGRADE8 = ''N''   AND PT.PRODUCTCODE = ''N0001659''  THEN ''S0579504''
                                                                                                       WHEN :V_IS_REGRADE9 = ''N''   AND PT.PRODUCTCODE = ''N0001664''  THEN ''S0579344''
                                                                                                       WHEN :V_IS_REGRADE10 = ''N''   AND PT.PRODUCTCODE = ''N0001662''  THEN ''S0579384''
                                                                                                   END '               
                                                                                                   || 'FROM NY_MIG_PRODUCT_TREE PT '				
                                                                                                   || ' WHERE FILE_ID = '''
                                                                                                   || V_FILE_ID
                                                                                                   || ''' and PRODUCTCODE = '''
                                                                                                   || C.PRODUCTCODE
                                                                                                   || ''' and vlocity_cmt__Action__c = '''
                                                                                                   || c.vlocity_cmt__Action__c
                                                                                                   || '''' INTO v_sltvalue USING V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE,V_IS_REGRADE;

                                                                        Exception   
                                                                        WHEN OTHERS THEN
                                                                        v_sltvalue := '';
                                                                        V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                        PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_T_QUERY);
                                                                        END;

                                                                END IF;   
                                    /* HOME SECURITY logic Added by RUDHRA KUMAR ON  14-08-22 ::ENDS*/

                                    /* Code changes ADDED for BritBoX AND APPLE TV on 24-JULY-22 by RUDHRA KUMAR  :: STARTS */

                                      /*          ELSIF upper(j.ATTRIBUTE_NAME) = upper('[PM.Ceasedate]') THEN

                                                   IF C.PRODUCTCODE IN ('N0001682','N0001651','N0001659','N0001664','N0001662') THEN

                                                                    BEGIN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;

																	EXECUTE IMMEDIATE 'SELECT CASE WHEN product_id = ''N0001659'' AND VLOCITY_CMT__ACTION__C = ''Disconnect'' THEN NG_CUSTOMERREQUESTEDDATE__C
                                                                                                   WHEN product_id = ''N0001682'' AND VLOCITY_CMT__ACTION__C = ''Disconnect'' THEN NG_CUSTOMERREQUESTEDDATE__C
                                                                                                   WHEN product_id = ''N0001651'' AND VLOCITY_CMT__ACTION__C = ''Disconnect'' THEN NG_CUSTOMERREQUESTEDDATE__C
                                                                                                   WHEN product_id = ''N0001664'' AND VLOCITY_CMT__ACTION__C = ''Disconnect'' THEN NG_CUSTOMERREQUESTEDDATE__C
                                                                                                   WHEN product_id = ''N0001662'' AND VLOCITY_CMT__ACTION__C = ''Disconnect'' THEN NG_CUSTOMERREQUESTEDDATE__C
                                                                                                            END' 
                                                                                                    || ' FROM NY_MIG_PRODUCT_TREE PT WHERE PT.ORDERNUMBER=''' 
                                                                                                    || V_ORDERNUMBER
                                                                                                    || ' AND PT.PRODUCTCODE IN (''N0001659'',''N0001682'',''N0001651'',''N0001664'',''N0001662'')'
																	INTO v_sltvalue; 

                                                                    Exception   
                                                                    WHEN OTHERS THEN
                                                                    v_sltvalue := '';
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_T_QUERY);
                                                                    END;
                                                         END IF;
                                                         */
                                                ELSIF upper(j.ATTRIBUTE_NAME) = upper('[PM.Fulfillmentinstruction]') THEN

                                                               IF C.PRODUCTCODE = 'N0001313' THEN

                                                                    BEGIN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;

																	EXECUTE IMMEDIATE 'SELECT case  WHEN PT.PRODUCT_ID = ''N0001313'' and (TMF.PRODUCTSPECIFICATIONID <> ''S0507144'' OR TMF.PRODUCTSPECIFICATIONID <> ''S0507145'') AND OP.fulfillmentInstruction = ''Subscribe''  THEN  ''Subscribe''
                                                                                                    WHEN PT.PRODUCT_ID = ''N0001313'' and OP.fulfillmentInstruction = ''AutoRenewOn'' AND OP.Cease_date IS NULL THEN ''AutoRenewOn''
                                                                                                    WHEN PT.PRODUCT_ID = ''N0001313'' and OP.fulfillmentInstruction = ''AutoRenewOff'' AND OP.Cease_date IS NOT NULL THEN ''AutoRenewOff''
                                                                                                    ELSE ''Subscribe'' END'
                                                                                                    || ' FROM NY_MIG_PRODUCT_TREE PT, NY_MIG_ORDER_PRODUCT OP, NY_MIG_TMF_ORDERPRODUCT TMF WHERE PT.FILE_ID=''' 
                                                                                                    || V_FILE_ID
                                                                                                    ||''' AND PT.ORDERNUMBER = TMF.ORDERNUMBER(+) AND TMF.PRODUCTSPECIFICATIONID (+)= PT.S_CODE AND PT.ORDERNUMBER = OP.ORDERNUMBER  AND OP.PRODUCTNAME = PT.PRODUCTNAME AND PT.PRODUCTCODE = OP.PRODUCTCODE AND PT.PRODUCTID = OP.PRODUCTID '
                                                                                                    || ' AND PT.PRODUCTCODE = '''
                                                                                                    || 'N0001313'''
																	INTO v_sltvalue; 

                                                                    Exception   
                                                                    WHEN OTHERS THEN
                                                                    v_sltvalue := '';
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_T_QUERY);
                                                                    END;

                                                             ELSIF C.PRODUCTCODE = 'N0001036' THEN

                                                                    BEGIN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;

                                                                EXECUTE IMMEDIATE  'SELECT case WHEN PT.PRODUCT_ID = ''N0001036'' and OP.fulfillmentInstruction is null and (TMF.PRODUCTSPECIFICATIONID <> ''S0573304'' OR TMF.PRODUCTSPECIFICATIONID <> ''S0578325'' OR TMF.PRODUCTSPECIFICATIONID <> ''S0578324'' OR TMF.PRODUCTSPECIFICATIONID <> ''S0578304'' ) THEN  ''Subscribe''
                                                                                                      WHEN PT.PRODUCT_ID = ''N0001036'' and OP.fulfillmentInstruction = ''AutoRenewOn'' AND OP.Cease_date IS NULL THEN ''AutoRenewOn''
                                                                                                      WHEN PT.PRODUCT_ID = ''N0001036'' and OP.fulfillmentInstruction = ''AutoRenewOff'' AND OP.Cease_date IS NOT NULL THEN ''AutoRenewOff''
                                                                                                      WHEN PT.PRODUCT_ID = ''N0001036'' and OP.fulfillmentInstruction is null and (TMF.PRODUCTSPECIFICATIONID = ''S0573304'' OR TMF.PRODUCTSPECIFICATIONID = ''S0578325'' OR TMF.PRODUCTSPECIFICATIONID = ''S0578324'' OR TMF.PRODUCTSPECIFICATIONID = ''S0578304'' ) THEN NULL
                                                                                                      ELSE ''Subscribe'' END'
                                                                                                      || ' FROM NY_MIG_PRODUCT_TREE PT, NY_MIG_ORDER_PRODUCT OP, NY_MIG_TMF_ORDERPRODUCT TMF WHERE PT.FILE_ID=''' 
                                                                                                      || V_FILE_ID
                                                                                                      ||''' AND PT.ORDERNUMBER = TMF.ORDERNUMBER(+) AND TMF.PRODUCTSPECIFICATIONID (+)= PT.S_CODE AND PT.ORDERNUMBER = OP.ORDERNUMBER  AND OP.PRODUCTNAME = PT.PRODUCTNAME AND PT.PRODUCTCODE = OP.PRODUCTCODE AND PT.PRODUCTID = OP.PRODUCTID '
                                                                                                      || ' AND PT.PRODUCTCODE = '''
                                                                                                      || 'N0001036'''
																	INTO v_sltvalue; 

                                                                    Exception   
                                                                    WHEN OTHERS THEN
                                                                  --  v_sltvalue := '';
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_T_QUERY);
                                                                    END;   

                                    /* Code changes ADDED for BritBox AND APPLE TV on 24-JULY-22 by RUDHRA KUMAR  :: ENDS */

                                                 /* HOME SECURITY logic Added by RUDHRA KUMAR ON  14-08-22 ::STARTS*/

                                                                 ELSIF C.PRODUCTCODE IN ('N0001682','N0001651','N0001659','N0001664','N0001662') THEN

                                                                            BEGIN
                                                                                     V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;

                                                                            EXECUTE IMMEDIATE 'SELECT CASE WHEN vlocity_cmt__Action__c = ''Add'' THEN ''Subscribe''
                                                                                                           WHEN vlocity_cmt__Action__c = ''Disconnect'' THEN ''Cancel'' END FROM '
                                                                                                           || 'NY_MIG_PRODUCT_TREE'				
                                                                                                           || ' WHERE FILE_ID = '''
                                                                                                           || V_FILE_ID
                                                                                                           || ''' and PRODUCTCODE = '''
                                                                                                           || C.PRODUCTCODE
                                                                                                           || ''' and vlocity_cmt__Action__c = '''
                                                                                                           || c.vlocity_cmt__Action__c
                                                                                                           || ''''
                                                                                                           INTO v_sltvalue; 

                                                                            Exception   
                                                                            WHEN OTHERS THEN
                                                                            v_sltvalue := '';
                                                                            V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_T_QUERY);
                                                                            END;                                        

                                       END IF;
                                       /* Code changes Added by by raju palle for NowTV ADDon START On 09-08-22*/ 
                                                ELSIF upper(j.ATTRIBUTE_NAME) = upper('[PM.NTVFulfillmentinstruction]') THEN
                                                                 v_sltvalue := '';

                                                                    BEGIN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;

																	EXECUTE IMMEDIATE 'SELECT  CASE  WHEN   fulfillmentInstruction =''Retain'' THEN  ''Read''
                                                                                                     WHEN   fulfillmentInstruction =''AutoRenewOff'' THEN ''AutoRenewOff''
                                                                                                     WHEN   fulfillmentInstruction =''AutoRenewOn'' THEN ''AutoRenewOn''
                                                                                                     WHEN   fulfillmentInstruction =''Replace'' THEN  ''Cancel''  END'
                                                                                                    || ' FROM NY_MIG_PRODUCT_TREE   WHERE FILE_ID=''' 
                                                                                                    || V_FILE_ID
                                                                                                    || ''' and ROWID = ''' 
                                                                                                    || c.rowid
                                                                                                    || ''''


																	INTO v_sltvalue; 

                                                                    Exception   
                                                                    WHEN OTHERS THEN
                                                                    v_sltvalue := '';
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_T_QUERY);
                                                                    END;
                                        /* Code changes Added by by raju palle for NowTV ADDon Ends On 09-08-22*/
                                                ELSE
                                                                    BEGIN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                        			EXECUTE IMMEDIATE 'SELECT '
																	                  || j.TABLE_COLUMN 
																					  || ' FROM ' 
																					  || j.TABLE_NAME 
																					  || ' WHERE FILE_ID = '''
																					  || V_FILE_ID 
																					  || ''' and ROWID = ''' 
																					  || c.rowid
																					  || ''''
																	INTO v_sltvalue;
                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;
                                                END IF;                                                                    
                                    /* Code changes ADDED for "AppleTV and BritBox" on 19-May-22 by Paul :: Ends */

                                    /*Code changes added by Raju Palle for NOWTV on 16-Aug-22 Starts*/
                                    ELSIF j.TABLE_NAME = 'NY_MIG_METADATA_ATTRIBUTES' THEN
                                                v_sltvalue := '';    

                                                          BEGIN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                        			EXECUTE IMMEDIATE 'SELECT '
																	                  || j.TABLE_COLUMN 
																					  || ' FROM ' 
																					  || j.TABLE_NAME 
																					  || ' WHERE FILE_ID = '''
																					  || V_FILE_ID 
																					  || ''' and productcode = ''' 
																					  || c.productcode
																					  || ''''
																	INTO v_sltvalue;
                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END; 
                                    /*Code changes added by Raju Palle for NOWTV on 16-Aug-22 Ends*/                     

                                                                ELSIF j.TABLE_NAME IS NOT NULL THEN
                                                                    BEGIN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;

                                                                   EXECUTE IMMEDIATE 'SELECT '
																					  || j.TABLE_COLUMN
																					  || ' FROM '
																					  || j.TABLE_NAME 
																					  || ' WHERE FILE_ID = '''
																					  || V_ORDERNUMBER
																					 || ''''
																	INTO v_sltvalue;

                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code || ' Table name => '|| j.TABLE_NAME || ' - Column name => '|| j.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;
                                                                ELSE 
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||j.driver_s_code ||' display name '|| j.ATTRIBUTE_DISPLAY_NAME || ' - default value => '|| j.DEFAULT_VALUE;
																	v_sltvalue := j.DEFAULT_VALUE;

															    END IF;

																If j.name is not null then 

																	pkg_ny_mig_exception_log.prc_workflow_log(v_file_id,
																											 v_ordernumber,
																											 'XML GENERATION',
																											 'PRC_XML_INSTANCECHARACTERSTICLIST',
																											 'orderItem - Attributes',
																											 'NEW',
																											 j.name
																											 || ' '
																											 || v_sltvalue,
																											 sysdate
																	);										


																			v_prev_sltvalue := j.previousvalue;
                                                                            /* Added by ranganath 24-08-22 for Attribute value change start */
																			IF v_sltvalue IS NULL  AND j.previousvalue is not null and upper(c.ov_action) = 'UPDATE' THEN
																				 v_sltvalue := j.previousvalue;
																			--	v_prev_sltvalue := '';
																			END IF;
                                                                            /* Added by ranganath 24-08-22 for Attribute value change end */
																			-- Regrade changes end 


-- Regrade changes end 

																	END IF;

	INSERT INTO NY_MIG_INSTANCECHARACTERSTICLIST 
    (MAPPING_TYPE,
    FILE_ID,
    ORDERNUMBER,
    PRODUCT_TYPE,
    S_CODE,
    S_CODE_NAME,
    PARENT_S_CODE,
    PARENT_NAME,
    ATTRIBUTE_NAME,
    ATTRIBUTE_VALUE,
    ATTRIBUTE_PREVIOUS_VALUE,
    ACTION,
    PROD_TREE_ROWID)
	VALUES(c.MAPPING_TYPE,
    v_file_id,
    v_ordernumber,
    c.PRODUCT_TYPE,
    c.S_CODE,
    c.NAME,
    j.PARENT_S_CODE,
    j.PARENT,
   j.ATTRIBUTE_DISPLAY_NAME,
   v_sltvalue,
   v_prev_sltvalue,
   (case 
    WHEN v_sltvalue = v_prev_sltvalue AND upper(v_action_value) = upper('UPDATE') THEN '-'
    WHEN v_sltvalue is null and v_prev_sltvalue is null AND upper(v_action_value) = upper('UPDATE') THEN '-'
    ELSE 
    v_action_value END) ,
    c.rowid);

                    commit;




END LOOP;---j loop

			elsif upper(c.mapping_type) ='CHILD' THEN

										FOR k IN ( SELECT AM.MAPPING_TYPE, AM.DRIVER_S_CODE, AM.DRIVER_S_CODE_NAME, AM.NAME, AM.VALUE, AM.PARENT_S_CODE, AM.PARENT, A.*, 
                                            -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
											--(CASE WHEN UPPER(c.VLOCITY_CMT__ACTION__C) = 'ADD' THEN NULL ELSE PC.PRODUCTCHARACTERISTICVALUE  END) as previousvalue
											(CASE WHEN UPPER(c.OV_ACTION) = 'ADD' THEN NULL ELSE PC.PRODUCTCHARACTERISTICVALUE  END) as previousvalue
													FROM DRIVER_ASSET_MANDATORY AM, NY_ATTRIBUTE_MAPPING_CONFIG A, NY_MIG_TMF_PRODUCTCHARACTERISTIC PC
											WHERE  PC.ORDERNUMBER(+) = V_ORDERNUMBER
												AND PC.PRODUCTSPECIFICATIONID(+) =  c.s_code
                                                AND PC.PARENTPRODUCTSPECIFICATIONID(+) = C.PARENTPRODUCTSPECIFICATIONID 
												AND upper(PC.PRODUCTCHARACTERISTICNAME(+)) = upper(AM.NAME)
												AND UPPER(AM.VALUE) = UPPER(A.ATTRIBUTE_NAME)
												AND UPPER(AM.MAPPING_TYPE) = 'ATTRIBUTE'
												AND AM.driver_s_code =  c.s_code
                                                -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
												--AND UPPER(c.VLOCITY_CMT__ACTION__C) <> 'DELETE' 
                                            	AND UPPER(c.OV_ACTION) <> 'DELETE'
                                            union all  -- Regrade changes for attributes start
                                             SELECT 'attribute' as MAPPING_TYPE, c.s_code AS DRIVER_S_CODE, c.NAME AS DRIVER_S_CODE_NAME, 
                                                PRODUCTCHARACTERISTICNAME as NAME, PRODUCTCHARACTERISTICVALUE as VALUE, 
                                                C.PARENT_S_CODE as PARENT_S_CODE, c.PARENT as PARENT, PRODUCTCHARACTERISTICNAME as ATTRIBUTE_NAME , 
                                                  PRODUCTCHARACTERISTICNAME as ATTRIBUTE_DISPLAY_NAME, null as TABLE_NAME, null as TABLE_COLUMN, null as BUSINESS_LOGIC, 
                                                PRODUCTCHARACTERISTICVALUE as DEFAULT_VALUE, NULL as previousvalue
                                                FROM NY_MIG_TMF_PRODUCTCHARACTERISTIC
                                                WHERE FILE_ID = V_FILE_ID
                                                AND PRODUCTSPECIFICATIONID =  c.s_code
                                                AND PARENTPRODUCTSPECIFICATIONID = C.PARENTPRODUCTSPECIFICATIONID 
                                                -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
												--and UPPER(c.VLOCITY_CMT__ACTION__C) = 'DELETE'   
                                                and UPPER(c.OV_ACTION) = 'DELETE'
                                                -- Regrade changes End												
										) 
										LOOP

									pkg_ny_mig_exception_log.prc_workflow_log(v_file_id,
																			v_ordernumber,
																			'CONFIGURE NY_MIG_INSTANCECHARACTERSTICLIST',
																			'PRC_XML_INSTANCECHARACTERSTICLIST',
																			'orderItem Child - Attributes',
																			'NEW',
																			'Child Attribute '||k.name,
																			sysdate
									);
                                                              -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
                                                              -- IF c.VLOCITY_CMT__ACTION__C IS NOT NULL THEN
                                                                 IF c.OV_ACTION IS NOT NULL THEN
                                                                    --v_action_value := (CASE WHEN UPPER(c.VLOCITY_CMT__ACTION__C) = 'ADD' THEN 'Create' END);
                                                                 -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
                                                                 -- v_action_value := c.VLOCITY_CMT__ACTION__C;
                                                                    v_action_value := c.OV_ACTION;
                                                                END IF;

																IF k.TABLE_NAME = 'NY_MIG_ORDER_ATTRIBUTES'	THEN 

                                                                    v_sltvalue := '';

                                                                V_ERR_LOG_MSG := 'Order Item child attributes xml Tag - s_code ' ||k.driver_s_code || ' Table name => '|| k.TABLE_NAME || ' - Column name => '|| k.TABLE_COLUMN;

                                                                    BEGIN
                                                                        EXECUTE IMMEDIATE  'SELECT attributevalue FROM ' 
                                                                                            || ' NY_MIG_ORDER_ATTRIBUTES OA ' 
                                                                                            || ' WHERE EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM WHERE AM.PRODUCT_ID = OA.PRODUCTCODE)'
                                                                                            || ' AND OA.FILE_ID = '''
                                                                                            || V_FILE_ID
                                                                                            || ''' and OA.PRODUCTCODE = ''' 
                                                                                            || c.PRODUCTCODE                                                                                    
                                                                                            || ''' and OA.attributename = ''' 
                                                                                            || k.TABLE_COLUMN 
                                                                                            || ''''																	
                                                                            INTO v_sltvalue;

                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item child attributes xml Tag - s_code ' ||k.driver_s_code || ' Table name => '|| k.TABLE_NAME || ' - Column name => '|| k.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;

/* Code changes ADDED for Number Port Scenarios on 18-May-22 by Kanagavel s : Starts */

/* Code changes ADDED for MANAGED INSTALL Scenarios on 21-JUNE-22 by RAJU/RUDHRA s : Starts */

                                                               ELSIF k.TABLE_NAME = 'NY_MIG_PRODUCT_TREE' THEN
                                                                                     v_sltvalue := '';

                                                                    BEGIN
                                                                    V_ERR_LOG_MSG := 'Order Item child attributes xml Tag - s_code ' ||k.driver_s_code || ' Table name => '|| k.TABLE_NAME || ' - Column name => '|| k.TABLE_COLUMN;
                                                        			EXECUTE IMMEDIATE 'SELECT '
																	                  || k.TABLE_COLUMN 
																					  || ' FROM ' 
																					  || k.TABLE_NAME 
																					  || ' WHERE FILE_ID = '''
																					  || V_FILE_ID 
																					  || ''' and ROWID = ''' 
																					  || c.rowid
																					  || ''''
																	INTO v_sltvalue;
                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item child attributes xml Tag - s_code ' ||k.driver_s_code || ' Table name => '|| k.TABLE_NAME || ' - Column name => '|| k.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;

/* Code changes ADDED for MANAGED INSTALL Scenarios on 21-JUNE-22 by RAJU/RUDHRA s : ENDS */

                                                                ELSIF k.TABLE_NAME = 'NY_MIG_RELATED_PARTY'	THEN 
                                                                    IF upper(K.ATTRIBUTE_DISPLAY_NAME) = upper('MNUM_Current_CP') Then 
                                                                            BEGIN
                                                                                EXECUTE IMMEDIATE 'SELECT RP_NAME FROM '
                                                                                                   || 'NY_MIG_RELATED_PARTY'				
                                                                                                   || ' WHERE FILE_ID = '''
                                                                                                   || V_FILE_ID
                                                                                                   || ''' and UPPER(RP_ROLE) = ''' 
                                                                                                   || 'CURRENTCP'''
                                                                                   INTO v_sltvalue;

                                                                            Exception
                                                                            WHEN OTHERS THEN
                                                                            V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||k.driver_s_code || ' Table name => '|| k.TABLE_NAME || ' - Column name => '|| k.TABLE_COLUMN;
                                                                            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                            END;

                                                                    ELSIF upper(K.ATTRIBUTE_DISPLAY_NAME) = upper('rangeHolder') Then 
                                                                            BEGIN
                                                                                EXECUTE IMMEDIATE 'SELECT RP_NAME FROM '
                                                                                                   || 'NY_MIG_RELATED_PARTY'				
                                                                                                   || ' WHERE FILE_ID = '''
                                                                                                   || V_FILE_ID
                                                                                                   || ''' and UPPER(RP_ROLE) = ''' 
                                                                                                   || 'RANGEHOLDER''' 
                                                                                    INTO v_sltvalue;

                                                                            Exception
                                                                            WHEN OTHERS THEN
                                                                            V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||k.driver_s_code || ' Table name => '|| k.TABLE_NAME || ' - Column name => '|| k.TABLE_COLUMN;
                                                                            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                            END;
                                                                    END IF;
/* Code changes ADDED for Number Port Scenarios on 18-May-22 by Kanagavel s : Ends*/	

																ELSIF k.TABLE_NAME IS NOT NULL THEN

                                                                        v_sltvalue := '';
                                                                            BEGIN
                                                                    V_ERR_LOG_MSG := 'Order Item child attributes xml Tag - s_code ' ||k.driver_s_code || ' Table name => '|| k.TABLE_NAME || ' - Column name => '|| k.TABLE_COLUMN;

																	EXECUTE IMMEDIATE 'SELECT '
																					  || k.TABLE_COLUMN
																					  || ' FROM '
																					  || k.TABLE_NAME 
																					  || ' WHERE FILE_ID = '''
																					  || V_FILE_ID
																					  || ''''
																	INTO v_sltvalue;

                                                                            Exception
                                                                            WHEN OTHERS THEN
                                                                            V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||k.driver_s_code || ' Table name => '|| k.TABLE_NAME || ' - Column name => '|| k.TABLE_COLUMN;
                                                                            PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_INSTANCECHARACTERSTICLIST', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                            END;

                                                                ELSE 
                                                                    V_ERR_LOG_MSG := 'Order Item child attributes xml Tag - s_code ' ||k.driver_s_code ||' display name '|| k.ATTRIBUTE_DISPLAY_NAME || ' - default value => '|| k.DEFAULT_VALUE;
																	v_sltvalue := k.DEFAULT_VALUE;

															    END IF;

																If k.name is not null then 

																	pkg_ny_mig_exception_log.prc_workflow_log(v_file_id,
																											 v_ordernumber,
																											 'CONFIGURE NY_MIG_INSTANCECHARACTERSTICLIST',
																											 'PRC_XML_INSTANCECHARACTERSTICLIST',
																											 'orderItem - Attributes',
																											 'NEW',
																											 k.name
																											 || ' '
																											 || v_sltvalue,
																											 sysdate
																	);										

																			-- Regrade changes start ravi 8jul
																			v_prev_sltvalue := k.previousvalue;
																			/* Added by ranganath 24-08-22 for Attribute value change start */
																			IF v_sltvalue IS NULL  AND k.previousvalue is not null and upper(c.ov_action) = 'UPDATE' THEN
																				 v_sltvalue := k.previousvalue;
																			--	v_prev_sltvalue := '';
																			END IF;
                                                                            /* Added by ranganath 24-08-22 for Attribute value change end */

																			-- Regrade changes end 

																	END IF;


	INSERT INTO NY_MIG_INSTANCECHARACTERSTICLIST 
    (MAPPING_TYPE,
    FILE_ID,
    ORDERNUMBER,
    PRODUCT_TYPE,
    S_CODE,
    S_CODE_NAME,
    PARENT_S_CODE,
    PARENT_NAME,
    ATTRIBUTE_NAME,
    ATTRIBUTE_VALUE,
    ATTRIBUTE_PREVIOUS_VALUE,
    ACTION,
    PROD_TREE_ROWID)
	VALUES(c.MAPPING_TYPE,
    v_file_id,
    v_ordernumber,
    c.PRODUCT_TYPE,
    c.S_CODE,
    c.NAME,
    k.PARENT_S_CODE,
    k.PARENT,
    k.ATTRIBUTE_DISPLAY_NAME,
    v_sltvalue,
    v_prev_sltvalue,
    (case 
    WHEN v_sltvalue = v_prev_sltvalue AND upper(v_action_value) = upper('UPDATE') THEN '-'
    WHEN v_sltvalue is null and v_prev_sltvalue is null AND upper(v_action_value) = upper('UPDATE') THEN '-'
    ELSE 
  v_action_value END) ,
    c.rowid);

         COMMIT;

    END LOOP;  --k loop

END IF;

END LOOP;  ---c loop


END PRC_XML_INSTANCECHARACTERSTICLIST;
/* PRC_XML_INSTANCECHARACTERSTICLIST is added for None and Amend scenario for attribute level by Bala/Kanagavel/Ranganath on 08-08-2022 END */
 procedure PRC_XML_INSTANCEIDENTIFIER(V_ORDERNUMBER in varchar2 ,V_FILE_ID in number )
IS
v_asset_instvalue         VARCHAR2(500);
v_link_flag               VARCHAR2(1);
v_tag_name                VARCHAR2(100);
v_sltvalue                VARCHAR2(500);
v_servicepointtmfvalue       varchar2(100);
V_IS_REGRADE                  varchar2(100);
v_service_pt_status       ny_mig_service_point_mapping%ROWTYPE;
v_tag_value               VARCHAR2(500);
v_instvalue               VARCHAR2(500);
v_link_name               VARCHAR2(30);
v_link_prd_scode          VARCHAR2(30);
V_ERR_LOG_MSG             VARCHAR2(1000);
v_srv_pt_seq              NUMBER;
v_link_instvalue          VARCHAR2(500);
V_DISCOUNT_LIST 		  VARCHAR2(1000);
v_parent_s_code           ny_mig_product_tree.parent_s_code%TYPE;


BEGIN 
      FOR v IN (
                SELECT  
                    rowid,parent_s_code
                FROM
                    ny_mig_product_tree
                WHERE
                    ordernumber = v_ordernumber
                    AND UPPER(mapping_type) = 'ROOT'
                    AND status = 'NEW'
                    ORDER BY 1
            ) 
			LOOP
				--v_item_cnt := v_item_cnt + 1;
                --v_parent := v.parent;
                v_parent_s_code := v.parent_s_code;
                --v_orderitempricestroutput := '';
				--v_orderitemstroutput := '';
                --v_orderitemstroutput1 := '';
                --v_orderitemnotestroutput := '';
FOR c IN (
    SELECT
        COUNT(*)
        OVER() AS cnt,
        ROWID,
        p.*,
        (
            CASE
            WHEN instr(
                ','
                || p.rules
                || ',', ',1,'
            ) > 0 THEN
            'DUMMY_' || p.s_code
            END
        )      AS asset_id,
        (
            CASE
            WHEN instr(
                ','
                || p.rules
                || ',', ',2,'
            ) > 0 THEN
            'Y'
            ELSE
            'N'
            END
        )      AS link_flag,
        (
            CASE
            WHEN REGEXP_LIKE ( upper(
                p.product_type
            ),
                               v_discount_list ) THEN
            p.product_type || p.s_code
            WHEN p.product_type IS NOT NULL THEN
            p.product_type || p.s_code
            ELSE
            p.s_code
            END
        )      AS unique_s_code /* Added by Paul on 04-May-2022 as part of C1 change for DISCOUNT */
    FROM
        ny_mig_product_tree p
    WHERE
        p.ordernumber = v_ordernumber
				   --and p.PARENT = v_parent
                   --and p.parent_s_code = v_parent_s_code
        AND ( CASE
        WHEN p.mapping_type = 'root'
             AND p.rowid = v.rowid THEN
        1
        WHEN p.mapping_type = 'child'
             AND p.parent_s_code = v_parent_s_code THEN
        1
        ELSE
        0
              END = 1 )
    ORDER BY
        p.mapping_type DESC /* Updated by Paul on 04-May-2022 as part of C1 change for DISCOUNT */
) LOOP
                            IF c.ASSET_ID IS NOT NULL THEN

v_asset_instvalue := ''''|| c.ASSET_ID|| '''';
--REGRADE CHANGE for Asset ID START                            
                          -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade  
                          --  IF UPPER(c.VLOCITY_CMT__ACTION__C) <> 'ADD' THEN
                              IF UPPER(c.OV_ACTION) <> 'ADD' THEN
BEGIN
SELECT DISTINCT ''''|| NY_MIG_TMF_ORDERPRODUCT.PRODUCTID|| ''''
INTO v_asset_instvalue
FROM NY_MIG_TMF_ORDERPRODUCT
WHERE ORDERNUMBER = C.ORDERNUMBER
AND PRODUCTSPECIFICATIONID = C.S_CODE
                                   AND PARENTPRODUCTSPECIFICATIONID = C.PARENTPRODUCTSPECIFICATIONID ;


EXCEPTION
WHEN NO_DATA_FOUND THEN
v_asset_instvalue := ''''|| c.ASSET_ID|| '''';
END;
END IF;
insert into NY_MIG_INSTANCE_IDENTIFIER ( MAPPING_TYPE ,
FILE_ID ,
ORDERNUMBER ,
PRODUCT_TYPE,
S_CODE  ,
S_CODE_NAME  ,
PARENT_S_CODE   ,
PARENT_NAME     ,
Instancevalue ,
Instancename   ,
PROD_TREE_ROWID   )
values(c.mapping_type,
v_file_id,
v_ordernumber,
c.PRODUCT_TYPE,
                c.S_CODE,
                c.NAME,
                c.PARENT_S_CODE,
c.PARENT,
REPLACE(v_asset_instvalue,chr(39),''),
'AssetId',
c.rowid);
END IF;

v_link_flag := c.link_flag;
IF v_link_flag = 'Y' THEN

FOR n IN ( select 
p.*
  from
NY_MIG_PRODUCT_LINK p
 where
p.ORDERNUMBER = v_ordernumber
AND P.PROD_TREE_ROWID = C.ROWID
)
LOOP
v_link_name := n.link_name;
v_link_prd_scode := n.LINK_PRODUCT_SCODE_VALUE;

IF v_link_name IS NOT NULL AND v_link_prd_scode IS NOT NULL THEN

pkg_ny_mig_exception_log.prc_workflow_log(v_file_id,
v_ordernumber,
'INSTANCE ID Extraction',
'PRC_XML_INSTANCEIDENTIFIER',
'orderItem - Field',
'NEW',
'Instance Identifier '||v_link_name,
sysdate
);

v_link_instvalue := '''' || v_link_prd_scode || '''';
END IF;
insert into NY_MIG_INSTANCE_IDENTIFIER ( MAPPING_TYPE ,
FILE_ID ,
ORDERNUMBER ,
PRODUCT_TYPE,
S_CODE  ,
S_CODE_NAME  ,
PARENT_S_CODE   ,
PARENT_NAME     ,
Instancevalue ,
Instancename   ,
PROD_TREE_ROWID   )
values(c.mapping_type,
v_file_id,
v_ordernumber,
c.PRODUCT_TYPE,
c.S_CODE,
c.NAME,
c.PARENT_S_CODE,
c.PARENT,
REPLACE(v_link_instvalue,chr(39),''),
REPLACE(v_link_name,chr(39),''),
c.rowid);

END LOOP;  --N END LOOP 
END IF;

IF upper(c.mapping_type) = 'ROOT' THEN

							-- Asset mandatory Field starts	
								FOR f IN ( SELECT AM.MAPPING_TYPE, AM.DRIVER_S_CODE, AM.DRIVER_S_CODE_NAME, AM.NAME, AM.VALUE, AM.PARENT_S_CODE, AM.PARENT, A.*
												FROM DRIVER_ASSET_MANDATORY AM, NY_FIELD_MAPPING_CONFIG A
												WHERE UPPER(AM.VALUE) = UPPER(A.FIELD_NAME)
												AND UPPER(AM.MAPPING_TYPE) = 'FIELD'
												AND AM.driver_s_code =  c.s_code
                                                 and upper(AM.NAME) = 'SERVICEPOINTINDEX'
												-- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
                                                --AND UPPER(c.VLOCITY_CMT__ACTION__C) <> 'DELETE' 
											    AND UPPER(c.OV_ACTION) <> 'DELETE' 
											   union all  -- Regrade changes for Mandatory fields start
											 SELECT 'Field' as MAPPING_TYPE, c.s_code AS DRIVER_S_CODE, c.NAME AS DRIVER_S_CODE_NAME, 
												PRODUCTINSTANCENAME as NAME, PRODUCTINSTANCEVALUE as VALUE, 
												C.PARENT_S_CODE as PARENT_S_CODE, c.PARENT as PARENT, PRODUCTINSTANCEVALUE as FIELD_NAME , 
												 -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
                                                 --CASE WHEN  UPPER(c.VLOCITY_CMT__ACTION__C) = 'UPDATE' THEN  'Previous'||PRODUCTINSTANCENAME
												   CASE WHEN  UPPER(c.OV_ACTION) = 'UPDATE' THEN  'Previous'||PRODUCTINSTANCENAME
												   	ELSE PRODUCTINSTANCENAME
												   END as    FIELD_DISPLAY_NAME, 
												null as TABLE_NAME, null as TABLE_COLUMN, null as BUSINESS_LOGIC, 
												PRODUCTINSTANCEVALUE as DEFAULT_VALUE
												FROM NY_MIG_TMF_PRODUCTINSTANCEIDS
												WHERE ORDERNUMBER = V_ORDERNUMBER
												AND PRODUCTSPECIFICATIONID =  c.s_code
                                                and PARENTPRODUCTSPECIFICATIONID = C.PARENTPRODUCTSPECIFICATIONID
                                                  and UPPER(PRODUCTINSTANCENAME) <> UPPER('ServicePointIndex') 
												-- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
                                                --AND 1 = (CASE WHEN UPPER(c.VLOCITY_CMT__ACTION__C) = 'UPDATE' and PRODUCTINSTANCENAME = 'Asset Id' then 0 
                                                AND 1 = (CASE WHEN UPPER(c.OV_ACTION) = 'UPDATE' and PRODUCTINSTANCENAME = 'Asset Id' then 0 
                                                ELSE 1 END)
                                                -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
                                                --AND UPPER(c.VLOCITY_CMT__ACTION__C) <> upper('Add')
                                                AND UPPER(c.OV_ACTION) <> upper('Add')
										) 
										LOOP

											pkg_ny_mig_exception_log.prc_workflow_log(v_file_id,
																					v_ordernumber,
																					'INSTANCE ID Extraction',
																					'PRC_XML_GENERATION',
																					'orderItem - Field',
																					'NEW',
																					'Root Field '||f.name,
																					sysdate
											);

												--If UPPER(f.DEFAULT_VALUE) = 'NY_SERVICE_PT_SEQ.NEXTVAL' THEN

												--v_instvalue := '''' || v_srv_pt_seq  || '''';
                                            If UPPER(f.FIELD_DISPLAY_NAME) = 'SERVICEPOINTINDEX' THEN
                                              
                                                 if upper(c.ov_action) = 'UPDATE'  THEN


                                                       select PRODUCTINSTANCEVALUE into v_servicepointtmfvalue from NY_MIG_TMF_PRODUCTINSTANCEIDS
                                                        where ordernumber = v_ordernumber
                                                        and PRODUCTSPECIFICATIONID = c.s_code
                                                        and upper(PRODUCTINSTANCENAME) = 'SERVICEPOINTINDEX';
                                                        v_instvalue := '''' || v_servicepointtmfvalue  || '''';
                                                     else
                                                    v_instvalue := '''' || v_srv_pt_seq  || '''';
                                                    
                                                end if;

                                                ELSIF f.TABLE_NAME = 'NY_MIG_ORDER_ATTRIBUTES'	THEN 
                                                    v_instvalue := '';
                                                /* Code changes ADDED for "AppleTV and BritBox" on 19-May-22 by Paul :: Starts */	
                                                    IF UPPER(f.FIELD_NAME) = UPPER('[FM.MobServiceId]') THEN 

                                                         IF C.PRODUCTCODE = 'N0001036' THEN

                                                                V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||f.driver_s_code || ' Table name => '|| f.TABLE_NAME || ' - Column name => '|| f.TABLE_COLUMN;

                                                                    BEGIN
                                                                        	EXECUTE IMMEDIATE  'SELECT attributevalue FROM ' 
                                                                                            || ' NY_MIG_ORDER_ATTRIBUTES OA ' 
                                                                                            || ' WHERE EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM WHERE AM.PRODUCT_ID = OA.PRODUCTCODE 
																							AND NVL(UPPER(AM.PRODUCT_TYPE),''APPLE'') = NVL(UPPER('''
                                                                                            || c.product_type
                                                                                            || '''),''APPLE''))'
                                                                                            || ' AND OA.ORDERNUMBER = '''
                                                                                            || v_ordernumber
                                                                                            || ''' and OA.PRODUCTCODE = ''' 
                                                                                            || 'N0001036'                                                                                    
                                                                                            || ''' and OA.attributename = ''' 
                                                                                            || f.TABLE_COLUMN 
                                                                                            || ''''	INTO v_instvalue;   /* NVL(UPPER(AM.PRODUCT_TYPE) Added by Bala , IF PartnerKey attribute = "APPLE" THEN  fetch the value under VEPC_ATTR_RT_MOBILENUMBER (not null)  and update it */

                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||f.driver_s_code || ' Table name => '|| f.TABLE_NAME || ' - Column name => '|| f.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_GENERATION', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;

                                                   ELSIF C.PRODUCTCODE = 'N0001178' THEN

                                                         v_instvalue := '';

                                                   END IF;      

                                                                v_instvalue := ''''||v_instvalue|| '''';
                                                /* Code changes ADDED for "AppleTV and BritBox" on 19-May-22 by Paul :: Ends */	
                                                    ELSE
                                                                V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||f.driver_s_code || ' Table name => '|| f.TABLE_NAME || ' - Column name => '|| f.TABLE_COLUMN;

                                                                    BEGIN
                                                                        EXECUTE IMMEDIATE  'SELECT attributevalue FROM ' 
                                                                                            || ' NY_MIG_ORDER_ATTRIBUTES OA ' 
                                                                                            || ' WHERE EXISTS (SELECT 1 FROM DRIVER_ASSET_MAPPING AM WHERE AM.PRODUCT_ID = OA.PRODUCTCODE AND UPPER(AM.PRODUCT_TYPE) = UPPER('''
                                                                                            || c.product_type
                                                                                            || '''))'
                                                                                            || ' AND OA.ORDERNUMBER = '''
                                                                                            || v_ordernumber
                                                                                            || ''' and OA.PRODUCTCODE = ''' 
                                                                                            || c.PRODUCTCODE                                                                                    
                                                                                            || ''' and OA.attributename = ''' 
                                                                                            || f.TABLE_COLUMN 
                                                                                            || ''''																	
                                                                            INTO v_instvalue;

                                                                    Exception
                                                                    WHEN OTHERS THEN
                                                                    V_ERR_LOG_MSG := 'Order Item root attributes xml Tag - s_code ' ||f.driver_s_code || ' Table name => '|| f.TABLE_NAME || ' - Column name => '|| f.TABLE_COLUMN;
                                                                    PKG_NY_MIG_EXCEPTION_LOG.PRC_ERROR_LOG(v_file_id,v_ordernumber,'PRC_XML_GENERATION', SYSDATE, 'ERROR_LOG', V_ERR_LOG_MSG);
                                                                    END;

                                                                    v_instvalue := ''''||v_instvalue|| '''';
                                                    END IF;
												elsif f.TABLE_NAME is not null THEN

												V_ERR_LOG_MSG := 'Order Item xml Tag - s_code ' ||f.driver_s_code || ' Table name => '|| f.TABLE_NAME || ' - Column name => '|| f.TABLE_COLUMN;

													EXECUTE IMMEDIATE 'SELECT '
																	|| f.TABLE_COLUMN
																	|| ' FROM '
																	|| f.TABLE_NAME 
																	|| ' WHERE ORDERNUMBER = '''
																	|| v_ordernumber
																	|| ''''

																	INTO v_instvalue;

													v_instvalue := ''''||v_instvalue|| '''';

												else

													v_instvalue := ''''||f.DEFAULT_VALUE|| '''';

												END IF;
												 If f.name is not null then 
												 v_instvalue :=v_instvalue ;
												 END IF;

insert into NY_MIG_INSTANCE_IDENTIFIER ( MAPPING_TYPE ,
FILE_ID ,
ORDERNUMBER ,
PRODUCT_TYPE,
S_CODE  ,
S_CODE_NAME  ,
PARENT_S_CODE   ,
PARENT_NAME     ,
Instancevalue ,
Instancename   ,
PROD_TREE_ROWID   )
values(c.mapping_type,
v_file_id,
v_ordernumber,
c.PRODUCT_TYPE,
c.S_CODE,
c.NAME,
c.PARENT_S_CODE,
c.PARENT,
REPLACE(v_instvalue,chr(39),''),
REPLACE(f.FIELD_DISPLAY_NAME,chr(39),''),
c.rowid);

END LOOP;    --F END LOOP
END IF;

IF upper(c.mapping_type) = 'CHILD' THEN

	FOR h IN ( SELECT AM.MAPPING_TYPE, AM.DRIVER_S_CODE, AM.DRIVER_S_CODE_NAME, AM.NAME, AM.VALUE, AM.PARENT_S_CODE, AM.PARENT, A.*
												FROM DRIVER_ASSET_MANDATORY AM, NY_FIELD_MAPPING_CONFIG A
												WHERE UPPER(AM.VALUE) = UPPER(A.FIELD_NAME)
												AND UPPER(AM.MAPPING_TYPE) = 'FIELD'
												AND AM.driver_s_code =  c.s_code
                                                and upper(AM.NAME) = 'SERVICEPOINTINDEX'
                                                -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
												--AND UPPER(c.VLOCITY_CMT__ACTION__C) <> 'DELETE' 
												AND UPPER(c.OV_ACTION) <> 'DELETE'
											union all  -- Regrade changes Instance Identifier start
											 SELECT 'Field' as MAPPING_TYPE, c.s_code AS DRIVER_S_CODE, c.NAME AS DRIVER_S_CODE_NAME, 
												PRODUCTINSTANCENAME as NAME, PRODUCTINSTANCEVALUE as VALUE, 
												C.PARENT_S_CODE as PARENT_S_CODE, c.PARENT as PARENT, PRODUCTINSTANCEVALUE as FIELD_NAME , 
												 -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
												 --CASE WHEN  UPPER(c.VLOCITY_CMT__ACTION__C) = 'UPDATE' THEN  'Previous'||PRODUCTINSTANCENAME
												 CASE WHEN  UPPER(c.OV_ACTION) = 'UPDATE' THEN  'Previous'||PRODUCTINSTANCENAME
												   ELSE PRODUCTINSTANCENAME
												   END as    FIELD_DISPLAY_NAME, 
												null as TABLE_NAME, null as TABLE_COLUMN, null as BUSINESS_LOGIC, 
												PRODUCTINSTANCEVALUE as DEFAULT_VALUE
												FROM NY_MIG_TMF_PRODUCTINSTANCEIDS
												WHERE ORDERNUMBER = V_ORDERNUMBER
												AND PRODUCTSPECIFICATIONID =  c.s_code
                                                and PARENTPRODUCTSPECIFICATIONID = C.PARENTPRODUCTSPECIFICATIONID
                                                 and UPPER(PRODUCTINSTANCENAME) <> UPPER('ServicePointIndex')
												-- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
												--AND 1 = (CASE WHEN UPPER(c.VLOCITY_CMT__ACTION__C) = 'UPDATE' and PRODUCTINSTANCENAME = 'Asset Id' then 0 
                                                AND 1 = (CASE WHEN UPPER(c.OV_ACTION) = 'UPDATE' and PRODUCTINSTANCENAME = 'Asset Id' then 0 
                                                ELSE 1 END)
                                                -- Changed column VLOCITY_CMT__ACTION__C to OV_ACTION for N2N regrade
												--AND UPPER(c.VLOCITY_CMT__ACTION__C) <> upper('Add')
												AND UPPER(c.OV_ACTION) <> upper('Add')
										) 
										LOOP

											pkg_ny_mig_exception_log.prc_workflow_log(v_file_id,
																					v_ordernumber,
																					'INSTANCE ID Extraction',
																					'PRC_XML_INSTANCEIDENTIFIER',
																					'orderItem - Field',
																					'NEW',
																					'Child Field '||h.name,
																					sysdate
											);

												--If UPPER(h.DEFAULT_VALUE) = 'NY_SERVICE_PT_SEQ.NEXTVAL' THEN

												--v_instvalue := '''' || v_srv_pt_seq  || '''';
                                               -- If UPPER(h.FIELD_DISPLAY_NAME) = 'SERVICEPOINTINDEX' THEN
                                          
                                                 --   if upper(c.ov_action) = 'UPDATE' THEN
                                                        If UPPER(h.FIELD_DISPLAY_NAME) = 'SERVICEPOINTINDEX' THEN
                                              
                                                 if upper(c.ov_action) = 'UPDATE'  THEN



                                                       select PRODUCTINSTANCEVALUE into v_servicepointtmfvalue from NY_MIG_TMF_PRODUCTINSTANCEIDS
                                                        where ordernumber = v_ordernumber
                                                        and PRODUCTSPECIFICATIONID = c.s_code
                                                        and upper(PRODUCTINSTANCENAME) = 'SERVICEPOINTINDEX';
                                                        v_instvalue := '''' || v_servicepointtmfvalue  || '''';
                                                     else
                                                    v_instvalue := '''' || v_srv_pt_seq  || '''';
                                                    
                                                end if;
                                                       

												elsif h.TABLE_NAME is not null THEN

												V_ERR_LOG_MSG := 'Order Item xml Tag - s_code ' ||h.driver_s_code || ' Table name => '|| h.TABLE_NAME || ' - Column name => '|| h.TABLE_COLUMN;

													EXECUTE IMMEDIATE 'SELECT '
																	|| h.TABLE_COLUMN
																	|| ' FROM '
																	|| h.TABLE_NAME 
																	|| ' WHERE ORDERNUMBER = '''
																	|| v_ordernumber
																	|| ''''

																	INTO v_instvalue;

													v_instvalue := ''''||v_instvalue|| '''';

												else

													v_instvalue := ''''||h.DEFAULT_VALUE|| '''';

												END IF;

											IF h.name is not null then 
											 v_instvalue := v_instvalue ;
											 END IF;


												insert into NY_MIG_INSTANCE_IDENTIFIER ( MAPPING_TYPE ,
FILE_ID ,
ORDERNUMBER ,
PRODUCT_TYPE,
S_CODE  ,
S_CODE_NAME  ,
PARENT_S_CODE   ,
PARENT_NAME     ,
Instancevalue ,
Instancename   ,
PROD_TREE_ROWID   )
values(c.mapping_type,
v_file_id,
v_ordernumber,
c.PRODUCT_TYPE,
c.S_CODE,
c.NAME,
c.PARENT_S_CODE,
c.PARENT,
REPLACE(v_instvalue,chr(39),''),
REPLACE(h.FIELD_DISPLAY_NAME,chr(39),''),
c.rowid);


END LOOP;---H END LOOP
END IF;

 FOR i in (SELECT * FROM NY_SERVICE_PT_MAPPING_CONFIG WHERE upper(SP_TYPE) = 'INSTANCEIDENTIFIER')
                LOOP
                
                            IF i.TABLE_NAME = 'NY_MIG_SERVICE_POINT_MAPPING' and i.TABLE_COLUMN = 'PRODUCT_NAME' THEN 
                                v_tag_value := ''''|| v_service_pt_status.product_name  || '''';
                                v_tag_name :=  ''''|| i.SP_DISPLAY_NAME || '''';
                            ELSIF UPPER(i.DEFAULT_VALUE) = 'NY_SERVICE_PT_SEQ.NEXTVAL' THEN
                                v_tag_value := ''''|| v_srv_pt_seq  || '''';
                                v_tag_name :=  ''''|| i.SP_DISPLAY_NAME || ''''; 
                                /* Added by ranganath for servicepointindex 01-09-22 :: start*/  
                            IF V_IS_REGRADE = 'Y' THEN
                            select PRODUCTINSTANCEVALUE into v_servicepointtmfvalue from NY_MIG_TMF_PRODUCTINSTANCEIDS
                                                            where ordernumber = v_ordernumber
                                                            and PRODUCTSPECIFICATIONID = 'S0145868'
                                                            and upper(PRODUCTINSTANCENAME) = 'SERVICEPOINTINDEX';
                                                            v_tag_value := '''' || v_servicepointtmfvalue  || '''';
                                                            v_tag_name :=  ''''|| i.SP_DISPLAY_NAME || '''';
                                    ELSE
                                            
                                                v_tag_value := '''' || v_srv_pt_seq  || '''';
                                                v_tag_name :=  ''''|| i.SP_DISPLAY_NAME || '''';
                                    END IF;
/* Added by ranganath for servicepointindex 01-09-22 :: end*/     
                            ELSIF i.TABLE_NAME IS NOT NULL AND i.TABLE_COLUMN IS NOT NULL THEN
							        EXECUTE IMMEDIATE 'SELECT '
                                                      || i.TABLE_COLUMN
                                                      || ' FROM '
                                                      || i.TABLE_NAME
                                                      || ' WHERE ordernumber = '''
                                                      || v_ordernumber
                                                      || ''''
                                    INTO v_tag_value;

                                v_tag_value := ''''|| v_tag_value  || '''';
                                v_tag_name :=  ''''|| i.SP_DISPLAY_NAME || '''';
                            ELSE 
                                v_tag_value := ''''|| i.DEFAULT_VALUE  || '''';
                                v_tag_name :=  ''''|| i.SP_DISPLAY_NAME || '''';
                            END IF;
insert into NY_MIG_INSTANCE_IDENTIFIER ( MAPPING_TYPE ,
FILE_ID ,
ORDERNUMBER ,
PRODUCT_TYPE,
S_CODE  ,
S_CODE_NAME  ,
PARENT_S_CODE   ,
PARENT_NAME     ,
Instancevalue ,
Instancename   ,
PROD_TREE_ROWID   )
values( c.mapping_type,
v_file_id,
v_ordernumber,
c.product_type,
c.s_code,
c.name,
c.parent_s_code,
c.parent,
REPLACE(v_tag_value,chr(39),''),
REPLACE(v_tag_name,chr(39),''),
c.rowid);



							END LOOP;--I LOOP END
                            



						END LOOP;---C LOOP END
                        end loop; ---v loop end
                        commit;
						END PRC_XML_INSTANCEIDENTIFIER;

END PKG_NY_MIG_SRC_EXTRACTION_LOAD;
 
 /********* Begin Procedure Script ************/ 
 BEGIN 
 	 var_out =
 	 
SELECT DISTINCT REGION_1 AS REGION, SUBREGION, COALESCE(PLAZA,'ND') AS PLAZA, 1 AS COUNTER 
FROM "_SYS_BIC"."T.Dashboard/CA_MIS_DASHBOARD_V2"
	(PLACEHOLDER."$$C_INPUT_TO$$" => CURRENT_DATE)
; 

END /********* End Procedure Script ************/
--Desconexiones a nivel de RGU
CREATE PROCEDURE "T1"."TVI_SP_RGU_DX"
(IN FROM_DATE VARCHAR(8),
 IN TO_DATE   VARCHAR(8))
LANGUAGE SQLSCRIPT AS
V_FROM_DATE VARCHAR(8) := '';
V_TO_DATE   VARCHAR(8) := '';
--******************************************
-- Actualizado por CLR 230715
--****************************************** 
--******************************************
BEGIN
--******************************************
-- SETEO FECHA
--******************************************
--INICIALIZACION DE FECHA EN CASO DE NO SER PROPORCIONADA 
IF :FROM_DATE = '' THEN
    V_FROM_DATE := TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD');
ELSE
    V_FROM_DATE := FROM_DATE;
END IF;

IF :TO_DATE = '' THEN
    V_TO_DATE := TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD');
ELSE
    V_TO_DATE := TO_DATE;
END IF;
--****************************************** 
-- Borramos registros existentes para evitar duplicidades
-- de Cliente
DELETE FROM  "T2"."HANA_STG1_RGU"
WHERE FECHA BETWEEN :V_FROM_DATE AND :V_TO_DATE
AND ORIG_ID = 4 AND "NO_DX_3M" IS NOT NULL;
--**********************************
INSERT INTO  "T2"."HANA_STG1_RGU"
(
"ORIG_ID",
"ID_REGION",
"ID_EMPRESA",
"CLIENTE",
"PLAY_CLI",
--"PLAY_ORD",
"PRODUCTO_ID",
"CATEGORIA",
"PRODUCTO",
"FECHA",
"MES_1",
"ANIO_1",
"NO_DX_3M"
)
--********

SELECT 
4 AS ORIG_ID, R.ID AS ID_REGION, 4 AS ID_EMPRESA, C.CLIENTE AS CLIENTE,
       C.PLAY AS PLAY_CLI,
   --    MAP (LENGTH (C.PLAY),  1, '1 PLAY',  2, '2 PLAY',  3, '3 PLAY') PLAY_ORD,
       P.ID AS PRODUCTO_ID, CATEGORIA, P.PRODUCTO,
         MIN (TO_CHAR (A.FECHA_CIERRE, 'YYYYMMDD')) AS FECHA,
         MIN (TO_CHAR (ADD_MONTHS (A.FECHA_CIERRE, 1), 'YYYYMMDD')) AS MES_1,
         MIN (TO_CHAR (ADD_MONTHS (A.FECHA_CIERRE, 12), 'YYYYMMDD')) AS ANIO_1,
          1 AS NO_DX_3M
    FROM 
        "T1"."HANA_STG1_MT_CLIENTES" C,
        "T1"."HANA_STG1_DIM_REGION" R,
        "T1"."HANA_STG1_MT_ORDENES" O,
        "T1"."HANA_STG1_DM_DX_RX" A,
        "T1"."HANA_STG1_CAT_CLI_ESTATUS" CS,
        "T1"."HANA_STG1_MT_SERVICIOS" S,   
        "T1"."HANA_STG1_CAT_PRODUCTOS" P,      
        "T1"."HANA_STG1_CAT_SERV_ESTATUS" SE
    WHERE    0=0
    AND C.ORIG_ID = 4
    AND R.ORIG_ID = 4
    AND O.ORIG_ID = 4
    AND A.ORIG_ID = 4
    AND CS.ORIG_ID = 4
    AND S.ORIG_ID = 4
    AND P.ORIG_ID = 4
    AND SE.ORIG_ID = 4
    AND A.CLIENTE = C.CLIENTE
    AND A.ROW_ID = O.ROW_ID
    AND A.FECHA_CIERRE >= TO_DATE (:V_FROM_DATE, 'DD/MM/YYYY')
    AND A.FECHA_CIERRE <= TO_DATE (:V_TO_DATE, 'DD/MM/YYYY')
    AND A.TIPO_ACTIVIDAD IN ('Deshabilitación DLS Red',
    'Deshabilitación Mas. Cobranza','Deshabilitación DLS Cobranza',
    'Deshabilitación Mas. Manual','Deshabilitación Masiva Red',
    'Deshabilitación DLS Manual','Orden de Suspensión')
    AND C.CLIENTE=S.CLIENTE_ID
    AND C.ESTATUS_CTE_ID=CS.ID
    AND S.PRODUCTO_ID=P.ID
    AND S.ESTATUS_ID=SE.ID
    AND C.CIUDAD_ID=R.ID
    AND UPPER(CS.ESTATUS) NOT IN ('CANCELADO')
    AND UPPER(P.CATEGORIA) IN ('VIDEO', 'TELEFONIA', 'INTERNET')
    AND UPPER(P.TIPO) IN ('SERVICIO')
    AND UPPER(SE.ESTATUS) NOT IN ('INACTIVO','CANCELADO') 
    AND UPPER(P.PRODUCTO) NOT IN('SERVICIOS ON DEMAND')
    GROUP BY C.CIUDAD_ID, C.PLAY, C.CLIENTE, R.ID, P.CATEGORIA, P.ID,
     P.PRODUCTO, A.FECHA_CIERRE;
 END;
create procedure T1.brm2_load_data_rgu2(
in in_fc_this date,         -- fecha de carga actual
in in_fc_last date,         -- fecha de carga anterior
in in_rgu integer        -- obtener los datos de la tabla F_RGUS
)
as begin
  declare v_this date;
  declare v_last date;
  declare v_fuente varchar(5);

/**************************************************************************
* TABLAS TEMPORALES
***************************************************************************/
  CREATE LOCAL TEMPORARY table #t_map_dates  (
            fecha_carga date,
            fecha_cal date,
            orig_id integer
  ) ;
  
  create local temporary table #t_data1(
        "CUENTA"    varchar(60),
        "CLIENTE"   varchar(30),
        "ANIO"      decimal(10),
        "MES"       decimal(10),
        "MES_CLAVE" decimal(10),
        clasificacion varchar(20),
        "DECODIFICADOR" varchar(60),
        edo_cuenta varchar(10),
        edo_decof  varchar(10),
        "EDO_PRODUCTO" varchar(10),
        "FECHA_CARGA"  timestamp,
        "FECHA_INICIO" timestamp,
        "FECHA_FIN"    timestamp,
        "HUB"          varchar(10), 
        "RAMA"         varchar(30),
        "NODO"         varchar(30),
        linea          varchar(30),
        producto       varchar(100),
        deal_producto  varchar(100),
        "ID_PRODUCTO"  decimal(28,8),
        "INTERNET"     decimal(10),
        "VIDEO"        decimal(10),
        "TELEFONIA"    decimal(10),
        "ID_DIAFIN"    int,       
        "ID_DIAINI"    int,
        id_equipo      VARCHAR(20),
        ID_FEC_CREA    int,
        "ID_FIN_PROD"  int,
        "SERIAL"       VARCHAR(30),       
        TELEFONO       varchar(20),
        "TIPO"         varchar(60),
        "VIA"          varchar(20),
        fecha_cal      date,        
        fuente         varchar(15),
        filtro_rgu     int,
        filtro_cliente int,
        id_prod_all    int,
        estado_final   int,
        company varchar(20),
        id_empresa  int
      )   ;

  create local temporary table #t_data(
        "CUENTA"    varchar(60),
        "CLIENTE"   varchar(30),
        "ANIO"      decimal(10),
        "MES"       decimal(10),
        "MES_CLAVE" decimal(10),
        clasificacion varchar(20),
        "DECODIFICADOR" varchar(60),
        edo_cuenta varchar(10),
        edo_decof  varchar(10),
        "EDO_PRODUCTO" varchar(10),
        "FECHA_CARGA"  timestamp,
        "FECHA_INICIO" timestamp,
        "FECHA_FIN"    timestamp,
        "HUB"          varchar(10), 
        "RAMA"         varchar(30),
        "NODO"         varchar(30),
        linea          varchar(30),
        producto       varchar(100),
        deal_producto  varchar(100),
        "ID_PRODUCTO"  decimal(28,8),
        "INTERNET"     decimal(10),
        "VIDEO"        decimal(10),
        "TELEFONIA"    decimal(10),
        "ID_DIAFIN"    int,       
        "ID_DIAINI"    int,
        id_equipo      VARCHAR(20),
        ID_FEC_CREA    int,
        "ID_FIN_PROD"  int,
        "SERIAL"       VARCHAR(30),       
        TELEFONO       varchar(20),
        "TIPO"         varchar(60),
        "VIA"          varchar(20),
        fuente         varchar(15),
        fecha_cal date,        
        company varchar(20),
        id_empresa  int
      );

  create local temporary table #t_rgu_rgu(
      fecha_carga date,
      cuenta varchar(60),
      cliente varchar(30),
      linea   varchar(30),
      producto varchar(100),
      filtro_rgu  int
  );

  create local temporary table #t_rgu_cli(
     fuente varchar(10),
     fecha_carga date, 
     cuenta varchar(60),
     cliente varchar(30),
     linea varchar(30),
     producto varchar(100),
     filtro_cliente int
  );

  create local temporary table #t_edo(
    fecha_carga date,
    cuenta varchar(60),
    EDO_CUENTA varchar(10),
    EDO_DECOF varchar(10),
    EDO_PRODUCTO varchar(10),
    video int,
    telefonia int,
    internet int
  );

  create local temporary table #t_play(
    fecha_carga date,
    cuenta varchar(60),
    edo_cuenta varchar(10),
    edo_producto varchar(10),
    edo_decof varchar(10),             
    combo_play varchar(10),
    nivel_play varchar(10)
  );
    
  v_this := add_days(in_fc_this,-1);
  v_last := add_days(in_fc_last,-1);
  v_fuente := 'RGUS';
  
  --borrar antes de insertar para evitar duplicados
  delete from "T1"."F_RGUS_WT"
  where fuente = :V_FUENTE;
  
  if in_rgu = 1 then 

--    tabla temporal con fecha de carga actual y
--    fecha de carga anterior                    
      insert into #t_map_dates 
      select 
          :in_fc_this     as fecha_carga,
          :v_this         as fecha_cal,
          orig_id
      from ( select top 1 id as orig_id from "T1"."HANA_STG1_CAT_ORG_ID" where id = 3 )
      union
      select
          :in_fc_last       as fecha_carga,
          :v_last           as fecha_cal,
          orig_id          
        from ( select top 1 id as orig_id from "T1"."HANA_STG1_CAT_ORG_ID" where id = 3)
        ;

--------------------------------------------------------------
--    DETERMINAR LA EMPRESA A PARTIR DE LA REGION
--------------------------------------------------------------
      t_regio =     
        select f.hub, r.id as region_id, r.region,e.id
        from (
          select distinct hub   
          from "T1"."F_RGUS_V"
          where fecha_carga = :in_fc_this
            or  fecha_carga = :in_fc_last    
        ) f
      left join  "T1"."HANA_STG1_DIM_REGION" r on
        f.hub = r.id_trans and
        r.orig_id = 3
      left join  "T1"."HANA_STG1_CAT_EMPRESA" e on
        e.region_empresa = r.region
      where 
        f.hub is not null;
      
      
      insert into #t_data 
        select distinct 
        --linea 1
        "CUENTA",upper("CLIENTE") as cliente,"ANIO","MES","MES_CLAVE",
        --linea 2
        null as clasificacion ,"DECODIFICADOR",
        case "EDO_CUENTA"
          when 10100 then 1
          when 10102 then 2
          when 10103 then 3
          else null
        end as edo_cuenta,
        case left("EDO_DECOF",5)
          when '10100' then 1
          when '10102' then 2
          when '10103' then 3
          else null
        end as edo_decof,
        "EDO_PRODUCTO",
        --linea 3
        f."FECHA_CARGA" ,
        "FECHA_INICIO","FECHA_FIN",f."HUB", 
        --linea 4
        "RAMA","NODO",
        upper("LINEA") as linea,upper("PRODUCTO") as producto,
        null as deal_producto,
        --linea 5
        "ID_PRODUCTO","INTERNET","VIDEO","TELEFONIA",null as "ID_DIAFIN",
        --linea 6
        null as "ID_DIAINI", left(id_equipo,20) as id_equipo,
        null AS "ID_FEC_CREA",null AS "ID_FIN_PROD",
        "SERIAL",
        --linea 7
        null AS TELEFONO,"TIPO","VIA",
        :V_FUENTE as fuente,
        md.fecha_cal as fecha_cal,
        -- obtener el id de la empresa a partir del nombre
        upper(company) as company,
        emp.id as id_empresa
        from  "T1"."F_RGUS_V" f
        join #t_map_dates md on
          f.fecha_carga = md.fecha_carga
        join "T1"."HANA_STG1_CAT_EMPRESA" emp on
         trim(upper( CASE 	WHEN upper(f.company) = 'TVI MONTERREY' 
          					THEN 'TVI' 
         					ELSE f.company END )) = upper(emp.empresa)
--          trim(upper(f.company)) = upper(emp.empresa)
        where f.fecha_carga = :in_fc_this
          or  f.fecha_carga = :in_fc_last;        
          
        /*
        join #t_map_dates md on
          f.fecha_carga = md.fecha_carga
        join :t_regio emp on
          f.hub = emp.hub
        where f.fecha_carga = :in_fc_this
          or  f.fecha_carga = :in_fc_last;
        */  
    
    drop table #t_map_dates;
          
    insert into #t_rgu_rgu 
    select distinct  
      f_rgus.fecha_carga,
      f_rgus.cuenta,
      f_rgus.cliente,
      upper(f_rgus.linea) as linea,
      upper(f_rgus.producto) as producto,
      1 as filtro_rgu       
    from #t_data f_rgus
    LEFT JOIN ( SELECT
         ID AS ID_PRODUCTO_BRM,
         LINEA AS LINEA_CAT,
         PRODUCTO AS PRODUCTO_BRM 
        FROM "T1"."HANA_STG1_BRM_CAT_PROD" ) PROD ON 
        UPPER(PROD.PRODUCTO_BRM) = UPPER(F_RGUS.PRODUCTO) AND 
        UPPER(PROD.LINEA_CAT) = UPPER(F_RGUS.LINEA) 
    left join ( select
         cli.orig_id as orig_id_cli,
         cli.cliente as cliente_mt ,
         subtcli.subtipo as subtipo 
        from "T1"."HANA_STG1_MT_CLIENTES" cli 
        left join "T1"."HANA_STG1_CAT_SUBTIPO_CLIENTE" subtcli on cli.subtipo_cte_id = subtcli.id 
        and cli.orig_id = subtcli.orig_id 
        where cli.orig_id = 3 ) cst on 
        f_rgus.cuenta = cst.cliente_mt 
    WHERE 
      fuente = :V_FUENTE and
      --OJO: NOT
      not(      
      ( ( CST.SUBTIPO <> 'Cuenta SIP' AND upper(CST.SUBTIPO) <> 'STAFF' ) 
        or 
        ( CST.SUBTIPO IS NULL) ) 
    and ( F_RGUS.CLIENTE <> '2' 
        or F_RGUS.CLIENTE is null) 
    and ( ( F_RGUS.CLIENTE in ( 
                'RESIDENTIAL',
                'CENTREX PYME',
                'CENTREX PYME - SCT' 
            )
            AND F_RGUS.LINEA LIKE 'VIDEO' 
            AND F_RGUS.PRODUCTO NOT LIKE '%COMERCIAL%' 
            AND F_RGUS.PRODUCTO is not null ) 
        OR ( F_RGUS.LINEA LIKE 'INTERNET' 
        --      Eliminado , si se consideran los RGU de sky
--          AND F_RGUS.PRODUCTO NOT LIKE 'Sky%' 
            --Se excluyen productos sky sin equipo
            AND  F_RGUS.PRODUCTO NOT IN ( 
                 'INTERNET DE ALTA VELOCIDAD AHORRO',
                 'INTERNET WIFI PYME',
                 'INTERNET ENLACE DEDICADO CRECE',
                 'INTERNET DE ALTA VELOCIDAD IMPULSO',
                 'INTERNET DE ALTA VELOCIDAD EMPRENDE' 
                 )               
            ) 
            --CONDICION SKY( DEBE PASAR)
            OR
--          ( F_RGUS.PRODUCTO LIKE 'SKY%' AND ID_EQUIPO IS NOT NULL)
            ( F_RGUS.PRODUCTO LIKE 'SKY%' AND DECODIFICADOR IS NOT NULL)
        OR ( F_RGUS.LINEA LIKE 'TELEFONIA' 
            AND F_RGUS.PRODUCTO NOT IN ( 'Telefonia Llamadas SIP',
             'TELEFONIA LLAMADAS SIP',
             'PLAN SOLUCIONES SIP AHORRO',
             'PLAN SOLUCIONES SIP CRECE',
             'PLAN SOLUCIONES SIP EMPRENDE',             
             'PLAN SOLUCIONES SIP IMPULSO') ) )
         );

    insert into #t_rgu_cli
    select distinct  
      f_rgus.fuente,f_rgus.fecha_carga,f_rgus.cuenta,f_rgus.cliente,
      upper(f_rgus.linea) as linea,
      upper(f_rgus.producto) as producto,
      1 as filtro_cliente
    from #t_data f_rgus
    LEFT JOIN ( SELECT
         ID AS ID_PRODUCTO_BRM,
         LINEA AS LINEA_CAT,
         PRODUCTO AS PRODUCTO_BRM 
        FROM "T1"."HANA_STG1_BRM_CAT_PROD" ) PROD ON UPPER(PROD.PRODUCTO_BRM) = UPPER(F_RGUS.PRODUCTO) 
    AND UPPER(PROD.LINEA_CAT) = UPPER(F_RGUS.LINEA) 
    left join ( select
         cli.orig_id as orig_id_cli,
         cli.cliente as cliente_mt ,
         subtcli.subtipo as subtipo 
        from "T1"."HANA_STG1_MT_CLIENTES" cli 
        left join "T1"."HANA_STG1_CAT_SUBTIPO_CLIENTE" subtcli on cli.subtipo_cte_id = subtcli.id 
        and cli.orig_id = subtcli.orig_id 
        where cli.orig_id = 3 ) cst on f_rgus.cuenta = cst.cliente_mt 
    WHERE
      F_RGUS.FUENTE = :V_FUENTE AND
--    OJO
      not( 
          ( upper(CST.SUBTIPO) <> 'CUENTA SIP' AND upper(CST.SUBTIPO) <> 'STAFF' ) 
          and 
          ( F_RGUS.CLIENTE <> '2'   or F_RGUS.CLIENTE is null) 
          and 
          ( 
            ( F_RGUS.CLIENTE in  
              ( 
                'RESIDENTIAL',
                'CENTREX PYME',
                'CENTREX PYME - SCT' 
              ) 
              AND F_RGUS.LINEA LIKE 'VIDEO' 
              AND F_RGUS.PRODUCTO NOT LIKE '%COMERCIAL%' 
              AND F_RGUS.PRODUCTO is not null 
            ) 
            OR ( F_RGUS.LINEA LIKE 'INTERNET' --      Eliminado , si se consideran los RGU de sky
                AND F_RGUS.PRODUCTO NOT LIKE 'SKY%' 
                AND F_RGUS.PRODUCTO NOT IN ( 
             'INTERNET DE ALTA VELOCIDAD AHORRO',             
             'INTERNET WIFI PYME',
             'INTERNET ENLACE DEDICADO CRECE',
             'INTERNET DE ALTA VELOCIDAD IMPULSO',
             'INTERNET DE ALTA VELOCIDAD EMPRENDE' ) ) 
            OR ( F_RGUS.LINEA LIKE 'TELEFONIA' 
                AND F_RGUS.PRODUCTO NOT IN ( 
             'TELEFONIA LLAMADAS SIP',
             'PLAN SOLUCIONES SIP AHORRO',
             'PLAN SOLUCIONES SIP CRECE',
             'PLAN SOLUCIONES SIP EMPRENDE',             
             'PLAN SOLUCIONES SIP IMPULSO') ) )
         );
      
      
      insert into #t_data1  
      select
        wt."CUENTA",wt."CLIENTE",wt."ANIO",wt."MES",wt."MES_CLAVE",
        wt."CLASIFICACION",wt."DECODIFICADOR",wt."EDO_CUENTA",wt."EDO_DECOF",wt."EDO_PRODUCTO",
        wt.fecha_carga,wt."FECHA_INICIO",wt."FECHA_FIN",wt."HUB",
        wt."RAMA",wt."NODO",wt."LINEA",wt."PRODUCTO",wt."DEAL_PRODUCTO",
        wt."ID_PRODUCTO",wt."INTERNET",wt."TELEFONIA",wt."VIDEO",wt."ID_DIAFIN",
        wt."ID_DIAINI",wt."ID_EQUIPO",wt."ID_FEC_CREA",wt."ID_FIN_PROD",wt."SERIAL",
        wt."TELEFONO",wt."TIPO",wt."VIA",wt.fecha_cal,
        wt."FUENTE",fr.filtro_rgu,fc.filtro_cliente, prod.id as id_prod_all,
        case 
          when edo_producto=1 then 
            case 
              when edo_decof =1 then 
                case 
                  when edo_cuenta = 1 then 1
                  else edo_cuenta 
                end          
              else edo_decof 
            end
          else edo_producto
        end as estado_final,
        company,id_empresa
      from #t_data wt
      left join #t_rgu_rgu fr on
            fr.fecha_carga = wt.fecha_carga
        and fr.cuenta      = wt.cuenta 
        and fr.cliente     = wt.cliente 
        and fr.linea       = wt.linea 
        and fr.producto    = wt.producto
      left join #t_rgu_cli fc on
            fc.fecha_carga = wt.fecha_carga
        and fc.cuenta      = wt.cuenta 
        and fc.cliente     = wt.cliente 
        and fc.linea       = wt.linea 
        and fc.producto    = wt.producto
      left join(
        select id, orig_id , upper(producto) as producto, categoria
        from "T1"."HANA_STG1_CAT_PRODUCTOS"
        where orig_id = 3      
      ) prod on 
        prod.producto = wt.producto;
        
      --liberar memoria
      drop table #t_data;
      drop table #t_rgu_rgu;
      drop table #t_rgu_cli;
      
      /**************************************************************************************   
      * obtener nivel play
      **************************************************************************************/      
      insert into #t_edo
            select
                a.fecha_carga,
                a.cuenta ,
                A.EDO_CUENTA,
                A.EDO_DECOF,
                A.EDO_PRODUCTO,
                SUM(video) video,
                SUM(telefonia) telefonia,
                SUM(internet) internet 
            from #t_data1 a 
            where edo_producto = 1
              and edo_decof = 1
              and edo_cuenta = 1
              and filtro_rgu is null
              and fuente = :V_FUENTE
              and fecha_carga between :in_fc_last and :in_fc_this
            group by 
             a.fecha_carga,
             a.cuenta ,
             A.EDO_CUENTA,
             A.EDO_DECOF,
             A.EDO_PRODUCTO;
      
      insert into #t_play
        SELECT
             fecha_carga,
             cuenta,
             edo_cuenta,
             edo_producto,
             edo_decof,
             CASE 
               WHEN video >= 1 AND internet IS NULL AND telefonia IS NULL THEN 'V' 
               WHEN video IS NULL AND internet >= 1 AND telefonia IS NULL THEN 'I' 
               WHEN video IS NULL AND internet IS NULL AND telefonia >= 1 THEN 'T' 
               WHEN video >= 1 AND internet >= 1 AND telefonia IS NULL THEN 'VI' 
               WHEN video >= 1 AND internet IS NULL AND telefonia >= 1 THEN 'VT' 
               WHEN video IS NULL AND internet >= 1 AND telefonia >= 1 THEN 'IT' 
               WHEN video >= 1 AND internet >= 1 AND telefonia >= 1 THEN 'VIT' 
             END combo_play,
             CASE 
               WHEN video >= 1 AND internet IS NULL AND telefonia IS NULL THEN '1 PLAY' 
               WHEN video IS NULL AND internet >= 1 AND telefonia IS NULL THEN '1 PLAY'  
               WHEN video IS NULL AND internet IS NULL AND telefonia >= 1 THEN '1 PLAY' 
               WHEN video >= 1 AND internet >= 1 AND telefonia IS NULL THEN '2 PLAY'  
               WHEN video >= 1 AND internet IS NULL AND telefonia >= 1 THEN '2 PLAY'  
               WHEN video IS NULL AND internet >= 1 AND telefonia >= 1 THEN '2 PLAY'  
               WHEN video >= 1 AND internet >= 1 AND telefonia >= 1 THEN '3 PLAY' 
             END nivel_play
        from  #t_edo ; 
      
      drop table #t_edo;
      
      /**************************************************************************************   
      * Guardar tabla limpia
      **************************************************************************************/            
      insert into "T1"."F_RGUS_WT" (
        "CUENTA","CLIENTE","ANIO","MES","MES_CLAVE",
        "CLASIFICACION","DECODIFICADOR","EDO_CUENTA","EDO_DECOF","EDO_PRODUCTO",
        "FECHA_CARGA","FECHA_INICIO","FECHA_FIN","HUB",
        "RAMA","NODO","LINEA","PRODUCTO","DEAL_PRODUCTO",
        "ID_PRODUCTO","INTERNET","TELEFONIA","VIDEO","ID_DIAFIN",
        "ID_DIAINI","ID_EQUIPO","ID_FEC_CREA","ID_FIN_PROD","SERIAL",
        "TELEFONO","TIPO","VIA","FECHA_CAL",
        "FUENTE","ESTADO_FINAL",filtro_rgu,filtro_cliente,id_prod_all,combo_play,nivel_play,
        COMPANY,ID_EMPRESA
      )
      select 
        wt."CUENTA",wt."CLIENTE",wt."ANIO",wt."MES",wt."MES_CLAVE",
        wt."CLASIFICACION",wt."DECODIFICADOR",wt."EDO_CUENTA",wt."EDO_DECOF",wt."EDO_PRODUCTO",
        wt.fecha_carga,wt."FECHA_INICIO",wt."FECHA_FIN",wt."HUB",
        wt."RAMA",wt."NODO",wt."LINEA",wt."PRODUCTO",wt."DEAL_PRODUCTO",
        wt."ID_PRODUCTO",wt."INTERNET",wt."TELEFONIA",wt."VIDEO",wt."ID_DIAFIN",
        wt."ID_DIAINI",wt."ID_EQUIPO",wt."ID_FEC_CREA",wt."ID_FIN_PROD",wt."SERIAL",
        wt."TELEFONO",wt."TIPO",wt."VIA",wt.fecha_cal,
        "FUENTE",estado_final,filtro_rgu,filtro_cliente,id_prod_all,combo_play,nivel_play,
         CASE 	WHEN upper(COMPANY) = 'TVI MONTERREY' 
          					THEN 'TVI' ELSE COMPANY END as COMPANY,ID_EMPRESA       
        from(
          select data.*,play.combo_play,play.nivel_play
          from #t_data1 data
          left join #t_play play on
            data.fecha_carga = play.fecha_carga and
            data.cuenta      = play.cuenta         
        ) wt;
    
    update "T1"."F_RGUS_WT" 
    set filtro_rgu = 1 
    where LINEA = 'INTERNET' 
      AND PRODUCTO LIKE 'SKY%' AND DECODIFICADOR IS NULL
    ;
    
--  liberar tablas internas
--  drop table #t_map_dates;
--  drop table #t_data;
--  drop table #t_rgu_rgu;
--  drop table #t_rgu_cli;
--  drop table #t_edo;

    drop table #t_play;
    drop table #t_data1;    
  end if;

end;
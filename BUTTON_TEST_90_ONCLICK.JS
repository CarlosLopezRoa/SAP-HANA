
g_serv = "INSTA";

TEXT_7.setText("Detalle nivel día Instalaciones:");
TEXT_PAN2.setText("Instalaciones");
PANEL_HUB_VENTAS.setVisible(false);
PANEL_HUB_INSTALACION.setVisible(true);
PANEL_HUB_PERDIDAS.setVisible(false);
PANEL_HUB_CANCELACIONES_REGION.setVisible(false);
PANEL_HUB_RGU.setVisible(false);

if (g_pdet == "TODOS"){

  if(g_cross_trend=="cross"){

    //ventas
    SIMPLECROSSTAB_SERVICIOS_VENTAS_TODOS.setVisible(false);
    SIMPLECROSSTAB_SERVICIO_VENTAS_TELE.setVisible(false);
    SIMPLECROSSTAB_SERVICIO_VENTAS_TELEFONIA.setVisible(false);
    SIMPLECROSSTAB_SERVICIO_VENTAS_INTERNET.setVisible(false);

    //instalaciones
    SIMPLECROSSTAB_SERVICIOS_INSTALACION_TODOS.setVisible(true);
    SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELE.setVisible(false);
    SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELEFONIA.setVisible(false);
    SIMPLECROSSTAB_SERVICIO_INSTALACIONES_INTERNET.setVisible(false);

    //perdidas
    SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TODOS.setVisible(false);
    SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELE.setVisible(false);
    SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELEFONIA.setVisible(false);
    SIMPLECROSSTAB_SERVICIOS_PERDIDAS_INTERNET.setVisible(false);

    //cancelaciones
    SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TODOS.setVisible(false);
    SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_INTERNET.setVisible(false);
    SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELEFONIA.setVisible(false);
    SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELE.setVisible(false);

    //RGU
    SIMPLECROSSTAB_SERVICIOS_RGU_TODOS.setVisible(false);
    SIMPLECROSSTAB_SERVICIOS_RGU_TELE.setVisible(false);
    SIMPLECROSSTAB_SERVICIOS_RGU_TELEFONIA.setVisible(false);
    SIMPLECROSSTAB_SERVICIOS_RGU_INTERNET.setVisible(false);

    ///////////MOSTRAR KPI
    //BOTON INTERNET

    KPITILE_INTERNET.setVisible(false);

    //BOTON telefonia
    KPITILEDETAIL_TELEFONIA.setVisible(false);

    //BOTON television
    KPITILEDETAIL_VIDEO.setVisible(false);

    //BOTON TODOS
    KPITILE_TODOS_VENTAS.setVisible(true);

 //TEXT_INFO.setText("g_pdet-todos,g_cross_trend(Instal.)="+g_pdet+","+g_cross_trend);
  //TEXT_INFO.setText("g_pdet-todos-cross(Instal.)="+g_cross_trend);

 }

 if(g_cross_trend=="trend"){
	//TEXT_INFO.setText("g_pdet-todos-trend(Instal.)="+g_cross_trend);

   //ventas
   SIMPLECROSSTAB_SERVICIOS_VENTAS_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_INTERNET.setVisible(false);

   //instalaciones
   SIMPLECROSSTAB_SERVICIOS_INSTALACION_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_INTERNET.setVisible(false);

   //perdidas
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_INTERNET.setVisible(false);

   //cancelaciones
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TODOS.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_INTERNET.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELE.setVisible(false);

   //RGU
   SIMPLECROSSTAB_SERVICIOS_RGU_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_INTERNET.setVisible(false);


  //RC - 01/06/2015
   DS_TREND.loadDataSource();
   DS_TREND.setFilter("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
   DS_TREND.setFilter("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
   DS_TREND.setFilter("PLAZA", DROPDOWN_HUB.getSelectedText());



   var fecha_seleccionada = DATEFIELD_1.getDate();
   var fecha_dia_anterior = Convert.subString(APPLICATION.getInfo().dateNowInternalFormat, 0,10);

   var mes_ant  = fecha_dia_anterior.substring(4,6);
   var anio_ant = fecha_dia_anterior.substring(0,4);
   var fecha_ant  = "";
   var fecha_ant1 = "";
   var mes_ant_primero = 0;

	 //Validaciones para Dia primero de cada mes:
	//  if(fecha_dia_anterior.substring(6,8)=="01"){
	//   var dia_ant_dia_primero = "";

	//   //Para: Enero, Febrero, Abril, Junio, Agosto, Septiembre ó Noviembre:
	//   if(mes_ant=="01"||mes_ant=="02"||mes_ant=="04"||mes_ant=="06"||mes_ant=="08"||mes_ant=="09"||mes_ant=="11"){
	//    dia_ant_dia_primero = "31";
	//   }
	//   //Marzo
	//   if(mes_ant=="03"){
	// dia_ant_dia_primero = "28";
	//   }

	//  //Mayo ó Julio u Octubre ó Diciembre
	//  if(mes_ant=="05"||mes_ant=="07"||mes_ant=="10"||mes_ant=="12"){
	//  dia_ant_dia_primero = "30";
	//  }
	//  mes_ant_primero     = Convert.stringToInt(mes_ant)-1;
	//  //Construimos la Fecha del Día anterior:
	//  //fecha_ant = anio_ant+mes_ant+dia_ant_dia_primero;
	//  if(mes_ant_primero >=1 && mes_ant_primero <= 9){
	//  	fecha_ant = anio_ant+"0"+mes_ant_primero+dia_ant_dia_primero;
	//  }
	//  else{
	//  	fecha_ant = anio_ant+mes_ant_primero+dia_ant_dia_primero;
	//  }
	// }
	// else{
	//  var dia_ant   = Convert.stringToInt(fecha_dia_anterior.substring(6,8))-1;
	//    if(dia_ant >=1 && mes_ant_primero <= 9){
	//  	    //dia_ant = "0"+dia_ant;
	//  	    fecha_ant1 = anio_ant+mes_ant+"0"+dia_ant;
	//    }
	//    else{
	//    	fecha_ant1 = anio_ant+mes_ant+dia_ant;
	//    }
	//       fecha_ant  = fecha_ant1;
	// }

   if(fecha_seleccionada!=""){
      APPLICATION.setVariableValueExt("C_INPUT_TO", fecha_seleccionada);
      //TEXT_INFO.setText("fecha_seleccionada="+fecha_seleccionada);
   }
   else {
     APPLICATION.setVariableValueExt("C_INPUT_TO", fecha_dia_anterior);
     //TEXT_INFO.setText("fecha_ant="+fecha_ant);
   }
  //RC - 01/06/2015

   //RC - 04/06/2015
   //Filtros para KPIs:
   DS_PLAZA.setFilter("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
   DS_PLAZA.setFilter("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
   DS_PLAZA.setFilter("PLAZA", DROPDOWN_HUB.getSelectedText());
   //RC - 04/06/2015

//Instalaciones:
CHART_1.setDataSelection({
	"[Measures]": ["INSTALACIONES", "INSTALACIONES_PY", "SERVICIOS_INSTALACIONS"]
});

 }

}

if (g_pdet == "TELEVISION"){

 if(g_cross_trend=="cross"){

   //ventas
   SIMPLECROSSTAB_SERVICIOS_VENTAS_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_INTERNET.setVisible(false);

   //instalaciones
   SIMPLECROSSTAB_SERVICIOS_INSTALACION_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELE.setVisible(true);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_INTERNET.setVisible(false);

   //perdidas
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_INTERNET.setVisible(false);

   //cancelaciones
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TODOS.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_INTERNET.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELE.setVisible(false);


   //RGU
   SIMPLECROSSTAB_SERVICIOS_RGU_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_INTERNET.setVisible(false);

   //////mostrar KPI
   //BOTON INTERNET
   KPITILE_INTERNET.setVisible(false);

   //BOTON telefonia
   KPITILEDETAIL_TELEFONIA.setVisible(false);

   //BOTON television
   KPITILEDETAIL_VIDEO.setVisible(true);

   //BOTON TODOS
   KPITILE_TODOS_VENTAS.setVisible(false);

   //TEXT_INFO.setText("g_pdet-tv,g_cross_trend(Instal.)="+g_pdet+","+g_cross_trend);
   //TEXT_INFO.setText("g_pdet-tv-cross(Instal.)="+g_cross_trend);

 }

 if(g_cross_trend=="trend"){
	//TEXT_INFO.setText("g_pdet-tv-trend(Instal.)="+g_cross_trend);

   //ventas
   SIMPLECROSSTAB_SERVICIOS_VENTAS_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_INTERNET.setVisible(false);

   //instalaciones
   SIMPLECROSSTAB_SERVICIOS_INSTALACION_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_INTERNET.setVisible(false);

   //perdidas
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_INTERNET.setVisible(false);

   //cancelaciones
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TODOS.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_INTERNET.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELE.setVisible(false);

   //RGU
   SIMPLECROSSTAB_SERVICIOS_RGU_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_INTERNET.setVisible(false);


  //RC - 01/06/2015
   DS_TREND.loadDataSource();
   DS_TREND.setFilter("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
   DS_TREND.setFilter("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
   DS_TREND.setFilter("PLAZA", DROPDOWN_HUB.getSelectedText());



   var fecha_seleccionada_tv = DATEFIELD_1.getDate();
   var fecha_dia_anterior_tv = Convert.subString(APPLICATION.getInfo().dateNowInternalFormat, 0,10);

   var mes_ant_tv  = fecha_dia_anterior_tv.substring(4,6);
   var anio_ant_tv = fecha_dia_anterior_tv.substring(0,4);
   var fecha_ant_tv  = "";
   var fecha_ant1_tv = "";
   var mes_ant_primero_tv = 0;

	 //Validaciones para Dia primero de cada mes:
	//  if(fecha_dia_anterior_tv.substring(6,8)=="01"){
	//   var dia_ant_dia_primero_tv = "";

	//   //Para: Enero, Febrero, Abril, Junio, Agosto, Septiembre ó Noviembre:
	//   if(mes_ant_tv=="01"||mes_ant_tv=="02"||mes_ant_tv=="04"||mes_ant_tv=="06"||mes_ant_tv=="08"||mes_ant_tv=="09"||mes_ant_tv=="11"){
	//    dia_ant_dia_primero_tv = "31";
	//   }
	//   //Marzo
	//   if(mes_ant_tv=="03"){
	// dia_ant_dia_primero_tv = "28";
	//   }

	//  //Mayo ó Julio u Octubre ó Diciembre
	//  if(mes_ant_tv=="05"||mes_ant_tv=="07"||mes_ant_tv=="10"||mes_ant_tv=="12"){
	//  dia_ant_dia_primero_tv = "30";
	//  }
	//  mes_ant_primero_tv     = Convert.stringToInt(mes_ant_tv)-1;
	//  //Construimos la Fecha del Día anterior:
	//  //fecha_ant = anio_ant+mes_ant+dia_ant_dia_primero;
	//  if(mes_ant_primero_tv >=1 && mes_ant_primero_tv <= 9){
	//  	fecha_ant_tv = anio_ant_tv+"0"+mes_ant_primero_tv+dia_ant_dia_primero_tv;
	//  }
	//  else{
	//  	fecha_ant_tv = anio_ant_tv+mes_ant_primero_tv+dia_ant_dia_primero_tv;
	//  }
	// }
	// else{
	//  var dia_ant_tv   = Convert.stringToInt(fecha_dia_anterior_tv.substring(6,8))-1;
	//    if(dia_ant_tv >=1 && mes_ant_primero_tv <= 9){
	//  	    //dia_ant = "0"+dia_ant;
	//  	    fecha_ant1_tv = anio_ant_tv+mes_ant_tv+"0"+dia_ant_tv;
	//    }
	//    else{
	//    	fecha_ant1_tv = anio_ant_tv+mes_ant_tv+dia_ant_tv;
	//    }
	//       fecha_ant_tv  = fecha_ant1_tv;
	// }

   if(fecha_seleccionada_tv!=""){
      APPLICATION.setVariableValueExt("C_INPUT_TO", fecha_seleccionada_tv);
      //TEXT_INFO.setText("fecha_seleccionada_tv="+fecha_seleccionada_tv);
   }
   else if(fecha_seleccionada_tv==""){
     APPLICATION.setVariableValueExt("C_INPUT_TO", fecha_dia_anterior_tv);
     //TEXT_INFO.setText("fecha_ant_tv="+fecha_ant_tv);
   }
  //RC - 01/06/2015


  //Instalaciones:
  CHART_1.setDataSelection({
	"[Measures]": ["INSTALACIONES_VIDEO", "INSTALACIONES_VIDEO_PY", "SERVICIOS_INSTALACIONS_VIDEO"]
  });

 }

}

if (g_pdet == "TELEFONIA"){

 if(g_cross_trend=="cross"){

    //ventas
    SIMPLECROSSTAB_SERVICIOS_VENTAS_TODOS.setVisible(false);
    SIMPLECROSSTAB_SERVICIO_VENTAS_TELE.setVisible(false);
    SIMPLECROSSTAB_SERVICIO_VENTAS_TELEFONIA.setVisible(false);
    SIMPLECROSSTAB_SERVICIO_VENTAS_INTERNET.setVisible(false);

    //instalaciones
    SIMPLECROSSTAB_SERVICIOS_INSTALACION_TODOS.setVisible(false);
    SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELE.setVisible(false);
    SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELEFONIA.setVisible(true);
    SIMPLECROSSTAB_SERVICIO_INSTALACIONES_INTERNET.setVisible(false);

    //perdidas
    SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TODOS.setVisible(false);
    SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELE.setVisible(false);
    SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELEFONIA.setVisible(false);
    SIMPLECROSSTAB_SERVICIOS_PERDIDAS_INTERNET.setVisible(false);

    //cancelaciones
    SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TODOS.setVisible(false);
    SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_INTERNET.setVisible(false);
    SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELEFONIA.setVisible(false);
    SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELE.setVisible(false);

    //RGU
    SIMPLECROSSTAB_SERVICIOS_RGU_TODOS.setVisible(false);
    SIMPLECROSSTAB_SERVICIOS_RGU_TELE.setVisible(false);
    SIMPLECROSSTAB_SERVICIOS_RGU_TELEFONIA.setVisible(false);
    SIMPLECROSSTAB_SERVICIOS_RGU_INTERNET.setVisible(false);

    //////mostrar KPI
    //BOTON INTERNET
    KPITILE_INTERNET.setVisible(false);

    //BOTON telefonia
    KPITILEDETAIL_TELEFONIA.setVisible(true);

    //BOTON television
    KPITILEDETAIL_VIDEO.setVisible(false);

    //BOTON TODOS
    KPITILE_TODOS_VENTAS.setVisible(false);
    //TEXT_INFO.setText("g_pdet-telef,g_cross_trend(Instal.)="+g_pdet+","+g_cross_trend);
    //TEXT_INFO.setText("g_pdet-telef(Instal.)="+g_cross_trend);

 }
 if(g_cross_trend=="trend"){
	//TEXT_INFO.setText("g_pdet-telef-trend(Instal.)="+g_cross_trend);

   //ventas
   SIMPLECROSSTAB_SERVICIOS_VENTAS_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_INTERNET.setVisible(false);

   //instalaciones
   SIMPLECROSSTAB_SERVICIOS_INSTALACION_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_INTERNET.setVisible(false);

   //perdidas
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_INTERNET.setVisible(false);

   //cancelaciones
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TODOS.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_INTERNET.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELE.setVisible(false);

   //RGU
   SIMPLECROSSTAB_SERVICIOS_RGU_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_INTERNET.setVisible(false);


  //RC - 01/06/2015
   DS_TREND.loadDataSource();
   DS_TREND.setFilter("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
   DS_TREND.setFilter("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
   DS_TREND.setFilter("PLAZA", DROPDOWN_HUB.getSelectedText());

   var fecha_seleccionada_tel = DATEFIELD_1.getDate();
   var fecha_dia_anterior_tel = Convert.subString(APPLICATION.getInfo().dateNowInternalFormat, 0,10);

   var mes_ant_tel  = fecha_dia_anterior_tel.substring(4,6);
   var anio_ant_tel = fecha_dia_anterior_tel.substring(0,4);
   var fecha_ant_tel  = "";
   var fecha_ant1_tel = "";
   var mes_ant_primero_tel = 0;

   //Validaciones para Dia primero de cada mes:
  //  if(fecha_dia_anterior_tel.substring(6,8)=="01"){
  //   var dia_ant_dia_primero_tel = "";
	//
  //   //Para: Enero, Febrero, Abril, Junio, Agosto, Septiembre ó Noviembre:
  //   if(mes_ant_tel=="01"||mes_ant_tel=="02"||mes_ant_tel=="04"||mes_ant_tel=="06"||mes_ant_tel=="08"||mes_ant_tel=="09"||mes_ant_tel=="11"){
 // 	   dia_ant_dia_primero_tel = "31";
  //   }
  //   //Marzo
  //   if(mes_ant_tel=="03"){
 // 	dia_ant_dia_primero_tel = "28";
  //   }
	//
  //  //Mayo ó Julio u Octubre ó Diciembre
  //  if(mes_ant_tel=="05"||mes_ant_tel=="07"||mes_ant_tel=="10"||mes_ant_tel=="12"){
 // 	 dia_ant_dia_primero_tel = "30";
  //  }
  //  mes_ant_primero_tel     = Convert.stringToInt(mes_ant_tel)-1;
  //  //Construimos la Fecha del Día anterior:
  //  //fecha_ant = anio_ant+mes_ant+dia_ant_dia_primero;
  //  if(mes_ant_primero_tel >=1 && mes_ant_primero_tel <= 9){
  //  	fecha_ant_tel = anio_ant_tel+"0"+mes_ant_primero_tel+dia_ant_dia_primero_tel;
  //  }
  //  else{
  //  	fecha_ant_tel = anio_ant_tel+mes_ant_primero_tel+dia_ant_dia_primero_tel;
  //  }
  // }
  // else{
  //  var dia_ant_tel   = Convert.stringToInt(fecha_dia_anterior_tel.substring(6,8))-1;
  //    if(dia_ant_tel >=1 && mes_ant_primero_tel <= 9){
  //  	    //dia_ant = "0"+dia_ant;
  //  	    fecha_ant1_tel = anio_ant_tel+mes_ant_tel+"0"+dia_ant_tel;
  //    }
  //    else{
  //    	fecha_ant1_tel = anio_ant_tel+mes_ant_tel+dia_ant_tel;
  //    }
  //       fecha_ant_tel  = fecha_ant1_tel;
  // }

   if(fecha_seleccionada_tel!=""){
      APPLICATION.setVariableValueExt("C_INPUT_TO", fecha_seleccionada_tel);
      //TEXT_INFO.setText("fecha_seleccionada_tel="+fecha_seleccionada_tel);
   }
   else if(fecha_seleccionada_tel==""){
     APPLICATION.setVariableValueExt("C_INPUT_TO", fecha_dia_anterior_tel);
     //TEXT_INFO.setText("fecha_ant_tel="+fecha_ant_tel);
   }
  //RC - 01/06/2015



  //Instalaciones:
  CHART_1.setDataSelection({
	"[Measures]": ["INSTALACIONES_TELEFONIA", "INSTALACIONES_TELEFONIA_PY", "SERVICIOS_INSTALACIONS_TELEFONIA"]
  });

 }

}

if (g_pdet == "INTERNET"){

 if(g_cross_trend=="cross"){

   //ventas
   SIMPLECROSSTAB_SERVICIOS_VENTAS_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_INTERNET.setVisible(false);

   //instalaciones
   SIMPLECROSSTAB_SERVICIOS_INSTALACION_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_INTERNET.setVisible(true);

   //perdidas
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_INTERNET.setVisible(false);

   //cancelaciones
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TODOS.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_INTERNET.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELE.setVisible(false);

   //RGU
   SIMPLECROSSTAB_SERVICIOS_RGU_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_INTERNET.setVisible(false);

   //////mostrar KPI
   //BOTON INTERNET
   KPITILE_INTERNET.setVisible(true);

   //BOTON telefonia
   KPITILEDETAIL_TELEFONIA.setVisible(false);

   //BOTON television
   KPITILEDETAIL_VIDEO.setVisible(false);

   //BOTON TODOS
   KPITILE_TODOS_VENTAS.setVisible(false);

 }
 if(g_cross_trend=="trend"){
	//TEXT_INFO.setText("g_pdet-inet-trend(Instal.)="+g_cross_trend);

   //ventas
   SIMPLECROSSTAB_SERVICIOS_VENTAS_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_VENTAS_INTERNET.setVisible(false);

   //instalaciones
   SIMPLECROSSTAB_SERVICIOS_INSTALACION_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIO_INSTALACIONES_INTERNET.setVisible(false);

   //perdidas
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_PERDIDAS_INTERNET.setVisible(false);

   //cancelaciones
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TODOS.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_INTERNET.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_REG_SERVICIOS_CANCELACIONES_TELE.setVisible(false);

   //RGU
   SIMPLECROSSTAB_SERVICIOS_RGU_TODOS.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_TELE.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_TELEFONIA.setVisible(false);
   SIMPLECROSSTAB_SERVICIOS_RGU_INTERNET.setVisible(false);

  //RC - 01/06/2015
   DS_TREND.loadDataSource();
   DS_TREND.setFilter("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
   DS_TREND.setFilter("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
   DS_TREND.setFilter("PLAZA", DROPDOWN_HUB.getSelectedText());



   var fecha_seleccionada_inet = DATEFIELD_1.getDate();
   var fecha_dia_anterior_inet = Convert.subString(APPLICATION.getInfo().dateNowInternalFormat, 0,10);

   var mes_ant_inet  = fecha_dia_anterior_inet.substring(4,6);
   var anio_ant_inet = fecha_dia_anterior_inet.substring(0,4);
   var fecha_ant_inet  = "";
   var fecha_ant1_inet = "";
   var mes_ant_primero_inet = 0;
	//
  //  //Validaciones para Dia primero de cada mes:
  //  if(fecha_dia_anterior_inet.substring(6,8)=="01"){
  //   var dia_ant_dia_primero_inet = "";
	//
  //   //Para: Enero, Febrero, Abril, Junio, Agosto, Septiembre ó Noviembre:
  //   if(mes_ant_inet=="01"||mes_ant_inet=="02"||mes_ant_inet=="04"||mes_ant_inet=="06"||mes_ant_inet=="08"||mes_ant_inet=="09"||mes_ant_inet=="11"){
 // 	   dia_ant_dia_primero_inet = "31";
  //   }
  //   //Marzo
  //   if(mes_ant_inet=="03"){
 // 	dia_ant_dia_primero_inet = "28";
  //   }
	//
  //  //Mayo ó Julio u Octubre ó Diciembre
  //  if(mes_ant_inet=="05"||mes_ant_inet=="07"||mes_ant_inet=="10"||mes_ant_inet=="12"){
 // 	 dia_ant_dia_primero_inet = "30";
  //  }
  //  mes_ant_primero_inet     = Convert.stringToInt(mes_ant_inet)-1;
  //  //Construimos la Fecha del Día anterior:
  //  //fecha_ant = anio_ant+mes_ant+dia_ant_dia_primero;
  //  if(mes_ant_primero_inet >=1 && mes_ant_primero_inet <= 9){
  //  	fecha_ant_inet = anio_ant_inet+"0"+mes_ant_primero_inet+dia_ant_dia_primero_inet;
  //  }
  //  else{
  //  	fecha_ant_inet = anio_ant_inet+mes_ant_primero_inet+dia_ant_dia_primero_inet;
  //  }
  // }
  // else{
  //  var dia_ant_inet   = Convert.stringToInt(fecha_dia_anterior_inet.substring(6,8))-1;
  //    if(dia_ant_inet >=1 && mes_ant_primero_inet <= 9){
  //  	    //dia_ant = "0"+dia_ant;
  //  	    fecha_ant1_inet = anio_ant_inet+mes_ant_inet+"0"+dia_ant_inet;
  //    }
  //    else{
  //    	fecha_ant1_inet = anio_ant_inet+mes_ant_inet+dia_ant_inet;
  //    }
  //       fecha_ant_inet  = fecha_ant1_inet;
  // }

   if(fecha_seleccionada_inet!=""){
      APPLICATION.setVariableValueExt("C_INPUT_TO", fecha_seleccionada_inet);
      //TEXT_INFO.setText("fecha_seleccionada_inet="+fecha_seleccionada_inet);
   }
   else if(fecha_seleccionada_inet==""){
     APPLICATION.setVariableValueExt("C_INPUT_TO", fecha_dia_anterior_inet);
     //TEXT_INFO.setText("fecha_ant_inet="+fecha_ant_inet);
   }
  //RC - 01/06/2015


  //Instalaciones:
  CHART_1.setDataSelection({
	"[Measures]": ["INSTALACIONES_INTERNET", "INSTALACIONES_INTERNET_PY", "SERVICIOS_INSTALACIONS_INTERNET"]
  });

 }

}

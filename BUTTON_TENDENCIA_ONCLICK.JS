g_cross_trend = "trend";
g_filter == "no";


  //RC - 27/05/2015
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
   

   if(fecha_seleccionada!=""){
      APPLICATION.setVariableValueExt("C_INPUT_TO", fecha_seleccionada);
      TEXT_INFO.setText("fecha_seleccionada="+fecha_seleccionada);        
   }
   else {
     APPLICATION.setVariableValueExt("C_INPUT_TO", fecha_dia_anterior); 
     TEXT_INFO.setText("fecha_ant="+fecha_dia_anterior);
   }   
  //RC - 27/05/2015


//fecha_seleccionada
PANEL_CUERPO_PRINCIPAL_2.setVisible(true);
PANEL_HUB_CHART.setVisible(true);
//RCBP-12/MAY/2015
//PANEL__HUB_FILTER_DETALLE.setVisible(false);
PANEL__HUB_FILTER_DETALLE.setVisible(true);
//RC-22/MAY/2015
BUTTON_TENDENCIA.setVisible(false);
//PANEL__HUB_FILTER_DETALLE.setHeight(396);
PANEL__HUB_FILTER_DETALLE.setHeight(421);
//RC-22/MAY/2015


//GNZ 19/05/15 
//IMAGE_CHECK.setVisible(false); 
IMAGE_CHECK.setVisible(true);
//GNZ 19/05/15 

//SELECCION DE LOS DATOS PARA TREND

if (g_serv == "VENTAS") {
  
PANEL_HUB_VENTAS.setVisible(false);
CHART_1.setDataSelection({
  "[Measures]": ["VENTAS_TOTALES", "VENTAS_TOTALES_PY", "SERVICIOS_VENTAS"]
});


//TEXT_PAN2.setText("Ventas");
}

else if (g_serv == "INSTA") {
  
PANEL_HUB_INSTALACION.setVisible(false);
CHART_1.setDataSelection({
  "[Measures]": ["INSTALACIONES", "INSTALACIONES_PY", "SERVICIOS_INSTALACIONS"]
});
//TEXT_PAN2.setText("Instalaciones");
}
if (g_serv == "PERD") {
  
PANEL_HUB_PERDIDAS.setVisible(false);
CHART_1.setDataSelection({
  "[Measures]": ["NO_PERDIDAS", "NO_PERDIDAS_PM", "NO_PERDIDAS_PY"]
});
//TEXT_PAN2.setText("Pérdidas");
}
if (g_serv == "CANCE") {
PANEL_HUB_CANCELACIONES_REGION.setVisible(false);
CHART_1.setDataSelection({
  "[Measures]": ["NO_CANCELACIONES", "NO_CANCELACIONES_PM", "NO_CANCELACIONES_PY"]
});
//TEXT_PAN2.setText("Pérdidas");
}
if (g_serv == "RGU") {
PANEL_HUB_RGU.setVisible(false);  
CHART_1.setDataSelection({
  "[Measures]": ["NO_RGU", "NO_RGU_PY", "SERVICIOS_RGU"]
});
//TEXT_PAN2.setText("RGU");
}

TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
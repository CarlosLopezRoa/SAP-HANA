var a = "";
a = DROPDOWN_HUB.getSelectedValue();

var hub = "";
var palabra1 = "";
var palabra2 = "";
var espacio = 0;

espacio = a.indexOf(" ");
if (espacio != -1 && g_drop != "a"){
palabra1 = a.substring(0,espacio-1);
palabra2 = a.substring(espacio+1);
hub = palabra1+" "+palabra2;
}
else if (espacio != -1 && g_drop == "a"){
palabra1 = a.substring(0,espacio);
palabra2 = a.substring(espacio+1);
hub = palabra1+" "+palabra2;
}
else {
 hub = a;
}

if (g_cross_trend == "cross") {
	DS_PLAZA.clearFilter("PLAZA");
	DS_PLAZA.setFilterExt("PLAZA",hub);
}

//GN
TEXT_EMPRESA_PANTALLA2.setText(DROPDOWN_EMPRESA.getSelectedText());
TEXT_SUBREGION_PANTALLA2.setText(DROPDOWN_SUBREGION.getSelectedText());
TEXT_HUB_PANTALLA2.setText(DROPDOWN_HUB.getSelectedText());
//GN

if (g_cross_trend == "trend") {
    g_filter == "si";

	DS_PLAZA.clearFilter("PLAZA");
	DS_PLAZA.setFilterExt("PLAZA",hub);

   var fecha_seleccionada = DATEFIELD_1.getDate();
   var fecha_dia_anterior = Convert.subString(APPLICATION.getInfo().dateNowInternalFormat, 0,10);

   var mes_ant  = fecha_dia_anterior.substring(4,6);
   var anio_ant = fecha_dia_anterior.substring(0,4);
   var fecha_ant  = "";
   var fecha_ant1 = "";
   var mes_ant_primero = 0;

  //  //Validaciones para Dia primero de cada mes:
  //  if(fecha_dia_anterior.substring(6,8)=="01"){
  //   var dia_ant_dia_primero = "";
  //
  //   //Para: Enero, Febrero, Abril, Junio, Agosto, Septiembre ó Noviembre:
  //   if(mes_ant=="01"||mes_ant=="02"||mes_ant=="04"||mes_ant=="06"||mes_ant=="08"||mes_ant=="09"||mes_ant=="11"){
 // 	   dia_ant_dia_primero = "31";
  //   }
  //   //Marzo
  //   if(mes_ant=="03"){
 // 	dia_ant_dia_primero = "28";
  //   }
  //
  //  //Mayo ó Julio u Octubre ó Diciembre
  //  if(mes_ant=="05"||mes_ant=="07"||mes_ant=="10"||mes_ant=="12"){
 // 	 dia_ant_dia_primero = "30";
  //  }
  //  mes_ant_primero     = Convert.stringToInt(mes_ant)-1;
  //  //Construimos la Fecha del Día anterior:
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

    DS_TREND.clearFilter("PLAZA");
	DS_TREND.setFilterExt("PLAZA",hub);

    TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
}

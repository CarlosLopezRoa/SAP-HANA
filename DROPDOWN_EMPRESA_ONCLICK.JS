g_drop = "a";


APPLICATION.createWarningMessage(g_cross_trend);

//02/06/2015
DS_REGION.setFilterExt("REGION",DROPDOWN_EMPRESA.getSelectedText());
DROPDOWN_SUBREGION.setItems(DS_REGION.getMemberList("SUBREGION", MemberPresentation.EXTERNAL_KEY, MemberDisplay.TEXT, 20, "TODOS"));
DROPDOWN_SUBREGION.setSelectedValue("TODOS");
DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.TEXT, 100, "TODOS"));
DROPDOWN_HUB.setSelectedValue("TODOS");
//02/06/2015

if (g_cross_trend == "cross") {

    DS_PLAZA.clearAllFilters();
    DS_PLAZA.setFilterExt("REGION_1",DROPDOWN_EMPRESA.getSelectedValue());
   //RC - 15/05/2015
   if( (DROPDOWN_EMPRESA.getSelectedText()=="IZZI") && (DROPDOWN_SUBREGION.getSelectedText()=="TODOS") ){
	   DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));
	   DROPDOWN_HUB.setSelectedValue("TODOS");

       TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());

}
else{

    //--
    if( (DROPDOWN_EMPRESA.getSelectedText()=="METROPOLITANA") && (DROPDOWN_SUBREGION.getSelectedText()=="TODOS") ){
           //Inicia--REGION: METROPOLITANA; SUBREGION: TODOS.
           DROPDOWN_EMPRESA.setSelectedValue("METROPOLITANA");
           DS_REGION.setFilter("REGION", "METROPOLITANA");
           DS_PLAZA.setFilterExt("REGION_1", "METROPOLITANA");
           DS_PLAZA.clearFilter("SUBREGION");
           DS_REGION.clearFilter("SUBREGION");
           DROPDOWN_SUBREGION.setSelectedValue("TODOS");
           DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));
	       DROPDOWN_HUB.setSelectedValue("TODOS");
           TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
           //Fin--REGION: METROPOLITANA; SUBREGION: TODOS.
    }
    if( (DROPDOWN_EMPRESA.getSelectedText()=="INTERIOR") && (DROPDOWN_SUBREGION.getSelectedText()=="TODOS") ){
    //Inicia--REGION: INTERIOR; SUBREGION: TODOS.
      DROPDOWN_EMPRESA.setSelectedValue("INTERIOR");
      DS_REGION.setFilter("REGION", "INTERIOR");
      DS_PLAZA.setFilterExt("REGION_1", "INTERIOR");
      DS_PLAZA.clearFilter("SUBREGION");
      DS_REGION.clearFilter("SUBREGION");
      DROPDOWN_SUBREGION.setItems(DS_REGION.getMemberList("SUBREGION", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 20,"TODOS"));
      DROPDOWN_SUBREGION.setSelectedValue("TODOS");
      DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));

      TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
      //Fin--REGION: INTERIOR; SUBREGION: TODOS.
    }
    if( (DROPDOWN_EMPRESA.getSelectedText()=="NORESTE") && (DROPDOWN_SUBREGION.getSelectedText()=="TODOS") ){
    //Inicia--REGION: NORESTE; SUBREGION: TODOS.
      DROPDOWN_EMPRESA.setSelectedValue("NORESTE");
      DS_REGION.setFilterExt("REGION", "NORESTE");
      DROPDOWN_SUBREGION.setItems(DS_REGION.getMemberList("SUBREGION", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 20,"TODOS"));
      DS_PLAZA.setFilterExt("REGION_1", "NORESTE");
      DS_PLAZA.setFilterExt("SUBREGION", "NORESTE");
      DS_REGION.setFilterExt("SUBREGION", "NORESTE");
      DROPDOWN_SUBREGION.setSelectedValue(DS_REGION.getFilterExt("SUBREGION"));
      DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));

      TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
    //Fin--REGION: NORESTE; SUBREGION: TODOS.
    }
   //RC - 04/05/2015
 }
}

//--- Inicio (g_cross_trend == "trend")
if (g_cross_trend == "trend") {
    g_filter == "si";

    //RC-05/06/2015
    DS_PLAZA.clearAllFilters();
    DS_PLAZA.setFilterExt("REGION_1",DROPDOWN_EMPRESA.getSelectedValue());
    //RC-05/06/2015

   //Inicio (Validaciones de Parametro-Grafica de Tendencia: C_INPUT_TO)
   var fecha_seleccionada = DATEFIELD_1.getDate();
   var fecha_dia_anterior = Convert.subString(APPLICATION.getInfo().dateNowInternalFormat, 0,10);

   var mes_ant  = fecha_dia_anterior.substring(4,6);
   var anio_ant = fecha_dia_anterior.substring(0,4);
   var fecha_ant  = "";
   var fecha_ant1 = "";
   var mes_ant_primero = 0;
  //
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
  }
  else {
     APPLICATION.setVariableValueExt("C_INPUT_TO", fecha_dia_anterior);
  }//Fin (Validaciones de Parametro-Grafica de Tendencia: C_INPUT_TO)
  //*****************************************************************************************************************************
   DS_TREND.clearAllFilters();

   //Inicio (Region: IZZI, SUBREGION: TODOS)
   if( (DROPDOWN_EMPRESA.getSelectedText()=="IZZI") && (DROPDOWN_SUBREGION.getSelectedText()=="TODOS") ){
	    DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));
	    DROPDOWN_HUB.setSelectedValue("TODOS");
       /*
	   //--RC 18/MAY/2015
       DS_PLAZA.setFilter("REGION", "IZZI");
       DS_PLAZA.setFilterExt("REGION_1", "IZZI");
       DS_PLAZA.clearFilter("SUBREGION");
       DS_REGION.clearFilter("SUBREGION");
	   //--RC 18/MAY/2015
	   DS_PLAZA.setFilterExt("PLAZA",DROPDOWN_HUB.getSelectedText());
	   */
	   //rc - 04/06/2015
       //Filtros para Grafica de Tendencia:
       //DS_TREND.setFilterExt("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
       //DS_TREND.setFilterExt("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
       //DS_TREND.setFilterExt("PLAZA", DROPDOWN_HUB.getSelectedText());
	   //rc - 04/06/2015

       //RC-05/06/2015
       //DS_TREND.clearAllFilters();
       DS_TREND.setFilterExt("REGION_1",DROPDOWN_EMPRESA.getSelectedValue());
       //RC-05/06/2015


       TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
	   //TEXT_INFO.setText("g_cross_trend,izzi"+g_cross_trend);
   }//Fin (Region: IZZI, SUBREGION: TODOS)
   //*****************************************************************************************************************************

  else{//Inicio Else (Region: METROPOLITANA,INTERIOR, NORESTE; SUBREGION: TODOS y Otras)
    //--Inicio (Region: METROPOLITANA, SUBREGION: TODOS)
    if( DROPDOWN_EMPRESA.getSelectedText()=="METROPOLITANA"){
    	if(DROPDOWN_SUBREGION.getSelectedText()=="TODOS" ){
           //Inicia--REGION: METROPOLITANA; SUBREGION: TODOS.
           //DROPDOWN_EMPRESA.setSelectedValue("METROPOLITANA");
           /*
           DS_PLAZA.setFilterExt("REGION_1", "METROPOLITANA");
           DS_PLAZA.setFilter("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
           DS_PLAZA.clearFilter("SUBREGION");
           DROPDOWN_SUBREGION.setItems(DS_PLAZA.getMemberList("SUBREGION", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 20,"TODOS"));
           DROPDOWN_SUBREGION.setSelectedValue("TODOS");
           DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));
	       DROPDOWN_HUB.setSelectedValue("TODOS");
	       //DS_PLAZA.setFilterExt("PLAZA",DROPDOWN_HUB.getSelectedText());
	       DS_PLAZA.setFilter("SUBREGION",DROPDOWN_SUBREGION.getSelectedText());
	       DS_PLAZA.setFilter("PLAZA",DROPDOWN_HUB.getSelectedText());

          //Filtros para Grafica de Tendencia:
          DS_TREND.setFilterExt("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
          DS_TREND.setFilterExt("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
          DS_TREND.setFilterExt("PLAZA", DROPDOWN_HUB.getSelectedText());
          */

           //RC-05/06/2015
           //Inicia--REGION: METROPOLITANA; SUBREGION: TODOS.
           DROPDOWN_EMPRESA.setSelectedValue("METROPOLITANA");
           DS_REGION.setFilter("REGION", "METROPOLITANA");
           DS_PLAZA.setFilterExt("REGION_1", "METROPOLITANA");
           DS_PLAZA.clearFilter("SUBREGION");
           DS_REGION.clearFilter("SUBREGION");
           DROPDOWN_SUBREGION.setSelectedValue("TODOS");
           DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));
	       DROPDOWN_HUB.setSelectedValue("TODOS");
           //TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
           //Fin--REGION: METROPOLITANA; SUBREGION: TODOS.

           DS_TREND.setFilterExt("REGION_1", "METROPOLITANA");
           DS_TREND.clearFilter("SUBREGION");
          //RC-05/06/2015

          TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
          }
         }//--Fin (Region: METROPOLITANA, SUBREGION: TODOS)

         else{
            //Inicia--REGION: METROPOLITANA; SUBREGION: Distinta de TODOS.
         	if(DROPDOWN_SUBREGION.getSelectedText()!="TODOS" ){
               DROPDOWN_SUBREGION.setItems(DS_REGION.getMemberList("SUBREGION", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 20,"TODOS"));
               DROPDOWN_SUBREGION.setSelectedValue(DROPDOWN_SUBREGION.getSelectedText());
               DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));
              /*
              //Filtros para KPIs:
              DS_PLAZA.setFilter("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
              DS_PLAZA.setFilter("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
              DS_PLAZA.setFilter("PLAZA", DROPDOWN_HUB.getSelectedText());

               //Filtros para Grafica de Tendencia:
               DS_TREND.setFilterExt("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
               DS_TREND.setFilterExt("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
               DS_TREND.setFilterExt("PLAZA", DROPDOWN_HUB.getSelectedText());

               DS_PLAZA.setFilterExt("PLAZA",DROPDOWN_HUB.getSelectedText());
               */


               TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());

            }//Fin--REGION: METROPOLITANA; SUBREGION: Distinta de TODOS.
         }//Fin--REGION: METROPOLITANA; SUBREGION: Distinta de TODOS.
    }//--Fin (Region: METROPOLITANA, SUBREGION: TODOS)
    //****************************************************************************************************************************************************

    //--Inicio (Region:INTERIOR, SUBREGION: TODOS)
    if( DROPDOWN_EMPRESA.getSelectedText()=="INTERIOR"){
          if(DROPDOWN_SUBREGION.getSelectedText()=="TODOS"){

    //04/06/2015
    //Inicia--REGION: INTERIOR; SUBREGION: TODOS.

    //Inicia--REGION: INTERIOR; SUBREGION: TODOS.
      DROPDOWN_EMPRESA.setSelectedValue("INTERIOR");
      DS_REGION.setFilter("REGION", "INTERIOR");
      DS_PLAZA.setFilterExt("REGION_1", "INTERIOR");
      DS_PLAZA.clearFilter("SUBREGION");
      DS_REGION.clearFilter("SUBREGION");
      DROPDOWN_SUBREGION.setItems(DS_REGION.getMemberList("SUBREGION", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 20,"TODOS"));
      DROPDOWN_SUBREGION.setSelectedValue("TODOS");
      DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));
    //Fin--REGION: INTERIOR; SUBREGION: TODOS.

      //TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
      //Fin--REGION: INTERIOR; SUBREGION: TODOS.

    /*
      DROPDOWN_EMPRESA.setSelectedValue("INTERIOR");
      DS_PLAZA.setFilterExt("REGION_1", "INTERIOR");
      DS_PLAZA.setFilterExt("SUBREGION", "INTERIOR");
      DROPDOWN_SUBREGION.setItems(DS_PLAZA.getMemberList("SUBREGION", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 20,"TODOS"));
      DROPDOWN_SUBREGION.setSelectedValue("TODOS");
      DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));
      DROPDOWN_HUB.setSelectedValue("TODOS");

      DS_PLAZA.setFilterExt("PLAZA",DROPDOWN_HUB.getSelectedText());
//04/06/2015
            //Filtros para Grafica de Tendencia:
            DS_TREND.setFilterExt("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
            DS_TREND.setFilterExt("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
            DS_TREND.setFilterExt("PLAZA", DROPDOWN_HUB.getSelectedText());
       */

      DS_TREND.setFilterExt("REGION_1", "INTERIOR");
      DS_TREND.clearFilter("SUBREGION");

            TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
          }//Fin--REGION: INTERIOR; SUBREGION: TODOS.
       else{//--Inicio (Region:INTERIOR, SUBREGION: Distntinta de TODOS)
          if(DROPDOWN_SUBREGION.getSelectedText()!="TODOS"){
             DROPDOWN_SUBREGION.setItems(DS_REGION.getMemberList("SUBREGION", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 20,"TODOS"));
             DROPDOWN_SUBREGION.setSelectedValue(DROPDOWN_SUBREGION.getSelectedText());
             DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));

             //Filtros para KPIs:
              DS_PLAZA.setFilter("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
              DS_PLAZA.setFilter("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
              DS_PLAZA.setFilter("PLAZA", DROPDOWN_HUB.getSelectedText());

             //Filtros para Grafica de Tendencia:
             DS_TREND.setFilterExt("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
             DS_TREND.setFilterExt("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
             DS_TREND.setFilterExt("PLAZA", DROPDOWN_HUB.getSelectedText());

             DS_PLAZA.setFilterExt("PLAZA",DROPDOWN_HUB.getSelectedText());


            //Filtros para Grafica de Tendencia:
            DS_TREND.setFilterExt("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
            DS_TREND.setFilterExt("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
            DS_TREND.setFilterExt("PLAZA", DROPDOWN_HUB.getSelectedText());

            TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
          }
       }//--Fin (Region:INTERIOR, SUBREGION: Distntinta de TODOS)
    }//--Fin (Region: INTERIOR, SUBREGION: TODOS)
    //****************************************************************************************************************************************************

    //--Inicio (Region:NORESTE, SUBREGION: TODOS)
    if( DROPDOWN_EMPRESA.getSelectedText()=="NORESTE"){
    	if(DROPDOWN_SUBREGION.getSelectedText()=="TODOS"){

    //Inicia--REGION: NORESTE; SUBREGION: TODOS.
      DROPDOWN_EMPRESA.setSelectedValue("NORESTE");
      DS_REGION.setFilterExt("REGION", "NORESTE");
      DROPDOWN_SUBREGION.setItems(DS_REGION.getMemberList("SUBREGION", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 20,"TODOS"));
      DS_PLAZA.setFilterExt("REGION_1", "NORESTE");
      DS_PLAZA.setFilterExt("SUBREGION", "NORESTE");
      DS_REGION.setFilterExt("SUBREGION", "NORESTE");
      DROPDOWN_SUBREGION.setSelectedValue(DS_REGION.getFilterExt("SUBREGION"));
      DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));

      DS_TREND.setFilterExt("REGION_1", "NORESTE");
      DS_TREND.setFilterExt("SUBREGION", "NORESTE");

      //TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
    //Fin--REGION: NORESTE; SUBREGION: TODOS.

        /*
    	//RC - 04/06/2015
        //Inicia--REGION: NORESTE; SUBREGION: TODOS.
        DROPDOWN_EMPRESA.setSelectedValue("NORESTE");
        DS_PLAZA.setFilterExt("REGION", "NORESTE");
        DROPDOWN_SUBREGION.setItems(DS_PLAZA.getMemberList("SUBREGION", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 20,"TODOS"));
        DS_PLAZA.setFilterExt("REGION_1", "NORESTE");
        DS_PLAZA.setFilterExt("SUBREGION", "NORESTE");
        DROPDOWN_SUBREGION.setSelectedValue(DS_REGION.getFilterExt("SUBREGION"));
        DROPDOWN_SUBREGION.setSelectedValue("TODOS");
        DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));
        DROPDOWN_HUB.setSelectedValue("TODOS");

        DS_PLAZA.setFilterExt("PLAZA",DROPDOWN_HUB.getSelectedText());
        //Fin--REGION: NORESTE; SUBREGION: TODOS.
    	//RC - 04/06/2015

        //Filtros para Grafica de Tendencia:
        DS_TREND.setFilterExt("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
        DS_TREND.setFilterExt("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
        DS_TREND.setFilterExt("PLAZA", DROPDOWN_HUB.getSelectedText());
        */
        TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());

    	}
        else{//--Inicio (Region:NORESTE, SUBREGION: Distntinta de TODOS)
           if(DROPDOWN_SUBREGION.getSelectedText()!="TODOS"){
              //Inicia--REGION: METROPOLITANA; SUBREGION: TODOS.
              DROPDOWN_SUBREGION.setItems(DS_REGION.getMemberList("SUBREGION", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 20,"TODOS"));
              DROPDOWN_SUBREGION.setSelectedValue(DROPDOWN_SUBREGION.getSelectedText());
              DROPDOWN_HUB.setItems(DS_PLAZA.getMemberList("PLAZA", MemberPresentation.EXTERNAL_KEY, MemberDisplay.KEY, 100,"TODOS"));
              //Filtros para KPIs:
              DS_PLAZA.setFilter("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
              DS_PLAZA.setFilter("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
              DS_PLAZA.setFilter("PLAZA", DROPDOWN_HUB.getSelectedText());

              //Filtros para Grafica de Tendencia:
              DS_TREND.setFilterExt("REGION_1", DROPDOWN_EMPRESA.getSelectedText());
              DS_TREND.setFilterExt("SUBREGION", DROPDOWN_SUBREGION.getSelectedText());
              DS_TREND.setFilterExt("PLAZA", DROPDOWN_HUB.getSelectedText());


              DS_PLAZA.setFilterExt("PLAZA",DROPDOWN_HUB.getSelectedText());

              TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
           //Fin--REGION: NORESTE; SUBREGION: TODOS.
          }//--Fin (Region:NORESTE, SUBREGION: Distntinta de TODOS)
       }//--Fin (Region:NORESTE, SUBREGION: TODOS)
  }//Fin Else (Region: METROPOLITANA,INTERIOR, NORESTE; SUBREGION: TODOS y Otras)
}//--- Fin (g_cross_trend == "trend")


//RC - 04/JUN/2015
TEXT_EMPRESA_PANTALLA2.setText(DROPDOWN_EMPRESA.getSelectedText());
TEXT_SUBREGION_PANTALLA2.setText(DROPDOWN_SUBREGION.getSelectedText());
TEXT_HUB_PANTALLA2.setText(DROPDOWN_HUB.getSelectedText());
//TEXT_INFO.setText();
//TEXT_INFO.setText(DROPDOWN_EMPRESA.getSelectedText()+" "+DROPDOWN_SUBREGION.getSelectedText()+" "+DROPDOWN_HUB.getSelectedText());
//RC - 04/JUN/2015

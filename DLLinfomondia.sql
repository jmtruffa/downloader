CREATE TABLE  IF NOT EXISTS infomond_base_monetaria  
(
date					date NOT NULL PRIMARY KEY,   
cta_cte					double precision not null,
circulacion_monetaria 	double precision not null,
base_monetaria_total 	double precision not null
);


CREATE TABLE  IF NOT EXISTS infomond_tasa_interes 
(
date			date NOT NULL PRIMARY KEY,   
pase_pasivo		double precision not null,
pase_activo 	double precision not null,
tasa  			double precision not null
);



CREATE TABLE  IF NOT EXISTS infomond_tipo_cambio 
(
date				date NOT NULL PRIMARY KEY,   
saldo_reserva_dolar	double precision not null,
tipo_cambio 		double precision not null
);


CREATE TABLE  IF NOT EXISTS infomond_agreg_monetarios 
(
date			date NOT NULL PRIMARY KEY,   
base_monetaria	double precision not null,
m2_privado 		double precision not null
);



CREATE TABLE  IF NOT EXISTS infomond_liquidez_entidades 
(
date			date NOT NULL PRIMARY KEY,   
liquidez		double precision not null
);



CREATE TABLE  IF NOT EXISTS infomond_tasa_interes_depositos 
(
date			date NOT NULL PRIMARY KEY,   
badlar			double precision not null,
hasta_100mil 	double precision not null,
tm20  			double precision not null
);




CREATE TABLE  IF NOT EXISTS infomond_tasa_interes_prestamos 
(
date				date NOT NULL PRIMARY KEY,   
adelanto_empresas	double precision not null,
personales	 		double precision not null
);


CREATE TABLE  IF NOT EXISTS infomond_depositos_privados_variaprom30d
(
date			date NOT NULL PRIMARY KEY,   
plazo_fijo		double precision not null,
total 			double precision not null,
vista  			double precision not null
);



CREATE TABLE  IF NOT EXISTS infomond_prestamos_privados_variaprom30d
(
date					date NOT NULL PRIMARY KEY,   
total					double precision not null,
personales_y_tarjetas 	double precision not null,
adelantos_y_documentos  double precision not null
);



CREATE TABLE  IF NOT EXISTS infomond_prest_dep_me_privados_variaprom30d
(
date			date NOT NULL PRIMARY KEY,   
prestamos		double precision not null,
depositos 		double precision not null
);


CREATE TABLE  IF NOT EXISTS infomond_pasivos_pesos_bcra 
(
date			date NOT NULL PRIMARY KEY,   
pasivo			double precision not null
);



CREATE TABLE  IF NOT EXISTS infomond_factores_variacion 
(
date				date NOT NULL PRIMARY KEY,   
compra_divisas		double precision not null,
tesoro_nacional 	double precision not null,
pases  				double precision not null,
operaciones_lefi	double precision not null,
otros 				double precision not null,
base_monetaria  	double precision not null
);

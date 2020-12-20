# -----------------------------------------------------------------------------#
# --- PASO 1. Inicializar entorno ----

# i. Borrar objetos existentes en el ambiente
rm(list = ls()); gc()

# ii. Configurar huso horario en UTC
Sys.setenv(TZ = "UTC")

# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 2. Leer archivos YML de configuracion y parametros ----

# i. Leer YML de configuracion
args <- base::commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  archivo.config <- args[1]
} else {
  # No vino el archivo de configuracion por linea de comandos.
  # Utilizo un archivo default
  archivo.config <- paste0(getwd(), "/configuration.yml")
}

if (! file.exists(archivo.config)) {
  stop(paste0("El archivo de configuracion de ", archivo.config, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de configuracion ", archivo.config, "...\n"))
  config <- yaml::yaml.load_file(archivo.config)
}

# ii. YML de parametros para los controles de calidad
if (length(args) > 1) {
  archivo.params <- args[2]
} else {
  # No vino el archivo de parametros por linea de comandos.
  # Utilizo un archivo default
  archivo.params <- paste0(getwd(), "/parameters.yml")
}
if (! file.exists(archivo.params)) {
  stop(paste0("El archivo de parametros de ", archivo.params, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de parametros ", archivo.params, "...\n"))
  config$params <- yaml::yaml.load_file(archivo.params)
}

rm(archivo.config, archivo.params, args); gc()

# ------------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# --- Paso 3: Cargar e instalar paquetes necesarios ----

if (!require("pacman")) install.packages("pacman", repos = 'http://cran.us.r-project.org')

# Instalar o cargar los paquetes necesarios
pacman::p_load("dplyr", "tidyr", "raster", "sf", "maptools", "ggplot2", "readxl",
               "caret", "xgboost", "superml", "yardstick", "iterators", "missForest")

# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 4. Cargar datos necesarios ----
# -----------------------------------------------------------------------------#
# Cargar datos necesarios

# Datos de train
train <- read.csv(glue::glue("{config$dir$input}/data_train.csv")) %>%
  dplyr::mutate(train = TRUE)
# Datos de test
test <- read.csv(glue::glue("{config$dir$input}/data_test.csv")) %>%
  dplyr::mutate(train = FALSE)
# Labels
labels <- read.csv(glue::glue("{config$dir$input}/Etiquetas.csv"))
# Dataset completo 
dataset <- train %>%
  rbind(test) %>%
  dplyr::left_join(., labels, by = c('Cultivo')) %>%
  sf::st_as_sf(., coords = c( 'Longitud', 'Latitud'), crs = config$params$projections$latlon) %>%
  dplyr::mutate(Campania = if_else(Campania == "18/19", 2018, 2019))

# Area de estudio
area_estudio <- sf::st_read(glue::glue("{config$dir$input}/Gral_Lopez/"),
                            layer = 'Gral_Lopez')

# Cargar valores de las imágenes satelitales de los puntos dados
load(glue::glue("{config$dir$input}/dataset_satelite_observado.RData"))
# Cargar valores de las imágenes satelitales de los puntos complementarios
load(glue::glue("{config$dir$input}/dataset_satelite_complemento.RData"))
# Cargar puntos complementarios 
load(glue::glue("{config$dir$input}/grid_points_90.RData"))

# ------------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# ---- Paso 5: Imputar datos faltantes ----
# ---------------------------------------------------------------------------- #


# Agregar informacion: etiquetas de los cultivos a los puntos complementarios
dataset_complementario <- grid_points %>%
  sf::st_transform(., crs = config$params$projections$latlon) %>%
  cbind(dataset_satelite_complemento) %>%
  sf::st_set_geometry(NULL) %>%
  tidyr::drop_na(.) 

# Agregar informacion: etiquetas de los cultivos a los puntos dados
dataset_satelite_observado <- dataset %>%
  sf::st_transform(., crs = config$params$projections$latlon) %>%
  cbind(dataset_satelite) %>%
  sf::st_set_geometry(NULL)

# Completar datos faltantes

# Impitar faltantes en train
dataset_satelite_observado_imputar_train <- dataset_satelite_observado %>%
  dplyr::filter(train) %>%
  dplyr::select(-Id, -CultivoId, - Elevacion, -Dataset, - Campania,
                - GlobalId, -train, -CultivoId, - Tipo, -Cultivo)

variables_train_indices <- dataset_satelite_observado_imputar_train %>%
  dplyr::select(!contains(config$params$gge$bandas))

variables_train <- dataset_satelite_observado_imputar_train %>%
  dplyr::select(contains(config$params$gge$bandas))

metadata_train <- dataset_satelite_observado %>%
  dplyr::filter(train)  %>%
  dplyr::select(Id, CultivoId, Elevacion, Dataset, Campania,
                GlobalId, train, CultivoId,  Tipo, Cultivo)


doMC::registerDoMC(min(ncol(variables_train) - 2, 6))
impdata_train <- missForest::missForest(variables_train, 
                                        ntree=100, 
                                        parallelize='forests')$ximp

imputed_train <- purrr::map_dfc(
  .x = 1:nrow(fechas.procesables),
  .f = function(imagen) {
    
    # Fecha a procesar
    fecha_i <- fechas.procesables[imagen,]
    
    # Bandas a utilizar
    bandas <- config$params$gge$bandas
    
    # fecha actual de preocsamiento
    fecha <- glue::glue("_{fecha_i$year}_{fecha_i$month}")
    # Anio agricola correspondiente
    campana <- anio_agr(as.Date(glue::glue("{fecha_i$year}-{fecha_i$month}-15")), 7)
    mes_campania <- (lubridate::month(as.Date(glue::glue("{fecha_i$year}-{fecha_i$month}-15"))) - 7) %% 12 + 1
    
    # Correccion de fecha por anio agricola
    fecha_campania <- glue::glue("_{campana}_{mes_campania}")
    # indices usados
    indices <- c("NGRDI", "TGI", "NDVI", "GNDVI", "EVI", "AVI", 
                 "SAVI", "NDWI", "MSI", "BSI", "MCARI", "RED", 'NDRE', "NDII")
    indices_fecha <- stringr::str_c(indices, fecha)
    indices_fechas_campanoa <- stringr::str_c(indices, fecha_campania)
    # bandas usadas
    bandas_fecha <- stringr::str_c(bandas, fecha)
    bandas_fecha_campania <- stringr::str_c(bandas, fecha_campania)
    
    
    variables.i <- impdata_train %>%
      dplyr::select(bandas_fecha) %>%
      setNames(bandas) %>%
      cbind(variables_train_indices %>% dplyr::select(indices_fecha) %>% setNames(indices)) %>%
      dplyr::mutate(
        NGRDI = (B3 - B4)/(B3 + B4),
        TGI = -0.5 * (190 * (B4 - B3) - 120 * (B4 - B2)),
        NDVI = (B8 - B4)/(B8 + B4),
        GNDVI = (B8 - B3)/(B8 + B3),
        EVI = 2.5 * (B8 - B4)/((B8 + 6 * B4 - 7.5 * B2) + 1),
        AVI = (B8 * (1 - B4)*(B8 - B4))^(1/3),
        SAVI = (B8 - B4) / (B8 + B4 + 0.428) * (1.428),
        NDWI = (B8 - B11) / (B8 + B11),
        MSI = B11/B8,
        #GCI = (B9/B3) - 1,
        BSI = (B11 + B4) - (B8 + B2) / (B11 + B4) + (B8 + B2),
        MCARI = ((B5 - B4) - 0.2 * (B5 - B3)) * (B5 / B4),
        RED = (B7 / B5)^-1,
        NDRE = (B8A - B5)/(B8A + B5),
        NDII = (B8A - B11) / (B8A + B11) 
      ) %>%
     # dplyr::rename_at(vars(bandas), ~bandas_fecha) %>%
     #  dplyr::rename_at(vars(indices), ~indices_fecha) %>%
      dplyr::rename_at(vars(bandas), ~bandas_fecha_campania) %>%
      dplyr::rename_at(vars(indices), ~indices_fechas_campanoa) %>%
      #mutate_at(vars(bandas_fecha), list(~ ifelse(Campania == campana, ., 0))) %>%
      #mutate_at(vars(indices_fecha), list(~ ifelse(Campania == campana, ., 0))) %>%
      dplyr::select(bandas_fecha_campania, indices_fechas_campanoa)  %>%
      mutate_all(~ifelse(is.na(.x), 0, .x))
    
  }
  
)
# Crear dataset de train sin faltantes
train_dataset <- cbind(metadata_train, imputed_train)


# Impitar faltantes en test ---
variables_test_indices <- dataset_satelite_observado %>%
  dplyr::filter(!train)   %>%
  dplyr::select(-Id, -CultivoId, -Elevacion, -Dataset, -Campania,
                -GlobalId, -train, -CultivoId,  -Tipo, -Cultivo) %>%
  dplyr::select(!contains(config$params$gge$bandas))

# Indices en test
variables_test <- dataset_satelite_observado %>%
  dplyr::filter(!train)  %>%
  dplyr::select(contains(config$params$gge$bandas))

metadata_test <- dataset_satelite_observado %>%
  dplyr::filter(!train)  %>%
  dplyr::select(Id, CultivoId, Elevacion, Dataset, Campania,
                GlobalId, train, CultivoId,  Tipo, Cultivo)


doMC::registerDoMC(min(ncol(variables_test) - 2, 6))
impdata_test <- missForest::missForest(variables_test, 
                                       ntree=100, 
                                       parallelize='forests')$ximp

imputed_test <- purrr::map_dfc(
  .x = 1:nrow(fechas.procesables),
  .f = function(imagen) {
    
    # Fecha a procesar
    fecha_i <- fechas.procesables[imagen,]
    
    # Bandas a utilizar
    bandas <- config$params$gge$bandas
    
    # fecha actual de preocsamiento
    fecha <- glue::glue("_{fecha_i$year}_{fecha_i$month}")
    # Anio agricola correspondiente
    campana <- anio_agr(as.Date(glue::glue("{fecha_i$year}-{fecha_i$month}-15")), 7)
    mes_campania <- (lubridate::month(as.Date(glue::glue("{fecha_i$year}-{fecha_i$month}-15"))) - 7) %% 12 + 1
    
    # Correccion de fecha por anio agricola
    fecha_campania <- glue::glue("_{campana}_{mes_campania}")
    # indices usados
    indices <- c("NGRDI", "TGI", "NDVI", "GNDVI", "EVI", "AVI", 
                 "SAVI", "NDWI", "MSI", "BSI", "MCARI", "RED", 'NDRE', "NDII")
    indices_fecha <- stringr::str_c(indices, fecha)
    indices_fechas_campanoa <- stringr::str_c(indices, fecha_campania)
    # bandas usadas
    bandas_fecha <- stringr::str_c(bandas, fecha)
    bandas_fecha_campania <- stringr::str_c(bandas, fecha_campania)
    
    
    variables.i <- impdata_test %>%
      dplyr::select(bandas_fecha) %>%
      setNames(bandas) %>%
      cbind(variables_test_indices %>% dplyr::select(indices_fecha) %>% setNames(indices)) %>%
      dplyr::mutate(
        NGRDI = (B3 - B4)/(B3 + B4),
        TGI = -0.5 * (190 * (B4 - B3) - 120 * (B4 - B2)),
        NDVI = (B8 - B4)/(B8 + B4),
        GNDVI = (B8 - B3)/(B8 + B3),
        EVI = 2.5 * (B8 - B4)/((B8 + 6 * B4 - 7.5 * B2) + 1),
        AVI = (B8 * (1 - B4)*(B8 - B4))^(1/3),
        SAVI = (B8 - B4) / (B8 + B4 + 0.428) * (1.428),
        NDWI = (B8 - B11) / (B8 + B11),
        MSI = B11/B8,
        #GCI = (B9/B3) - 1,
        BSI = (B11 + B4) - (B8 + B2) / (B11 + B4) + (B8 + B2),
        MCARI = ((B5 - B4) - 0.2 * (B5 - B3)) * (B5 / B4),
        RED = (B7 / B5)^-1,
        NDRE = (B8A - B5)/(B8A + B5),
        NDII = (B8A - B11) / (B8A + B11) 
      ) %>%
      # dplyr::rename_at(vars(bandas), ~bandas_fecha) %>%
      #  dplyr::rename_at(vars(indices), ~indices_fecha) %>%
      dplyr::rename_at(vars(bandas), ~bandas_fecha_campania) %>%
      dplyr::rename_at(vars(indices), ~indices_fechas_campanoa) %>%
      #mutate_at(vars(bandas_fecha), list(~ ifelse(Campania == campana, ., 0))) %>%
      #mutate_at(vars(indices_fecha), list(~ ifelse(Campania == campana, ., 0))) %>%
      dplyr::select(bandas_fecha_campania, indices_fechas_campanoa)  %>%
      mutate_all(~ifelse(is.na(.x), 0, .x))
    
  }
  
)

# Crear dataset de test
test_dataset <- cbind(metadata_test, imputed_test)


# Corregir fechas datos de datelite complementarios
# Dataset complementario para los puntos subsampleado
dataset_satelite_complemento_campania <- purrr::map_dfc(
  .x = 1:nrow(fechas.procesables),
  .f = function(imagen) {
    
    # Fecha a procesar
    fecha_i <- fechas.procesables[imagen,]
    
    # Bandas a utilizar
    bandas <- config$params$gge$bandas
    
    # fecha actual de preocsamiento
    fecha <- glue::glue("_{fecha_i$year}_{fecha_i$month}")
    # Anio agricola correspondiente
    campana <- anio_agr(as.Date(glue::glue("{fecha_i$year}-{fecha_i$month}-15")), 7)
    mes_campania <- (lubridate::month(as.Date(glue::glue("{fecha_i$year}-{fecha_i$month}-15"))) - 7) %% 12 + 1
    
    # Correccion de fecha por anio agricola
    fecha_campania <- glue::glue("_{campana}_{mes_campania}")
    # indices usados
    indices <- c("NGRDI", "TGI", "NDVI", "GNDVI", "EVI", "AVI", 
                 "SAVI", "NDWI", "MSI", "BSI", "MCARI", "RED", 'NDRE', "NDII")
    indices_fecha <- stringr::str_c(indices, fecha)
    indices_fechas_campanoa <- stringr::str_c(indices, fecha_campania)
    # bandas usadas
    bandas_fecha <- stringr::str_c(bandas, fecha)
    bandas_fecha_campania <- stringr::str_c(bandas, fecha_campania)
    
    
    variables.i <- dataset_satelite_complemento %>%
      mutate(Tipo = grid_points %>% dplyr::pull(Tipo)) %>%
      dplyr::select(Tipo, bandas_fecha) %>%
      dplyr::group_by(Tipo) %>%
      mutate_all(~ifelse(is.na(.x), median(.x, na.rm = TRUE), .x)) %>%  
      dplyr::ungroup(.) %>%
      dplyr::select(-Tipo) %>%
      setNames(bandas) %>%
      #cbind(dataset_satelite_complemento %>% dplyr::select(indices_fecha) %>% setNames(indices)) %>%
      dplyr::mutate(
        NGRDI = (B3 - B4)/(B3 + B4),
        TGI = -0.5 * (190 * (B4 - B3) - 120 * (B4 - B2)),
        NDVI = (B8 - B4)/(B8 + B4),
        GNDVI = (B8 - B3)/(B8 + B3),
        EVI = 2.5 * (B8 - B4)/((B8 + 6 * B4 - 7.5 * B2) + 1),
        AVI = (B8 * (1 - B4)*(B8 - B4))^(1/3),
        SAVI = (B8 - B4) / (B8 + B4 + 0.428) * (1.428),
        NDWI = (B8 - B11) / (B8 + B11),
        MSI = B11/B8,
        #GCI = (B9/B3) - 1,
        BSI = (B11 + B4) - (B8 + B2) / (B11 + B4) + (B8 + B2),
        MCARI = ((B5 - B4) - 0.2 * (B5 - B3)) * (B5 / B4),
        RED = (B7 / B5)^-1,
        NDRE = (B8A - B5)/(B8A + B5),
        NDII = (B8A - B11) / (B8A + B11) 
      ) %>%
      # dplyr::rename_at(vars(bandas), ~bandas_fecha) %>%
      #  dplyr::rename_at(vars(indices), ~indices_fecha) %>%
      dplyr::rename_at(vars(bandas), ~bandas_fecha_campania) %>%
      dplyr::rename_at(vars(indices), ~indices_fechas_campanoa) %>%
      #mutate_at(vars(bandas_fecha), list(~ ifelse(Campania == campana, ., 0))) %>%
      #mutate_at(vars(indices_fecha), list(~ ifelse(Campania == campana, ., 0))) %>%
      dplyr::select(bandas_fecha_campania, indices_fechas_campanoa) %>%
      mutate_all(~ifelse(is.na(.x), 0, .x))
    
    
  }
)
# -----------------------------------------------------------------------------

# ----------------------------------------------------------------------------- #
# --- Paso 6: Generar features de interes ----
# ----------------------------------------------------------------------------- #

# indice <- 'GNDVI'

dataset_auxiliar <- train_dataset %>%
  rbind(test_dataset) %>%
  dplyr::select(Id, Cultivo, Elevacion, Dataset, Campania,
                GlobalId, train, CultivoId, Tipo, everything())

dataset_complementario_clase <- grid_points %>%
  sf::st_transform(., crs = config$params$projections$latlon) %>%
  cbind(dataset_satelite_complemento_campania) %>%
  sf::st_set_geometry(NULL) %>%
  tidyr::drop_na(.) 

indices <- c("NGRDI", "TGI", "NDVI", "GNDVI", "EVI", "AVI", 
             "SAVI", "NDWI", "MSI", "BSI", "MCARI", "RED", 'NDRE', "NDII")


dataset_auxiliar <- rbind(dataset_auxiliar, dataset_complementario_clase) %>%
  dplyr::distinct() %>%
  dplyr::select(Id, Cultivo, Elevacion, Dataset, Campania,
                GlobalId, train, CultivoId, Tipo, contains(indices)) %>%
 # dplyr::select(!contains('BSI')) %>%
 # dplyr::select(Id, Cultivo, Elevacion, Dataset, Campania,
 #                GlobalId, train, CultivoId, Tipo, contains('2018')) %>%
  dplyr::distinct(.) %>%
  dplyr::select(!contains('2020'))


# Septiembre, Octubre, Diciembre, Abril, Mayo, Junio
dataset_resumen <- purrr::map_dfc(
  .x = indices,
  .f = function(indice) {
    
    # Verano
    verano_2018 <- c(paste0(indice, "_2018_6"), paste0(indice, "_2018_7"), paste0(indice, "_2018_8"))
    verano_2019 <- c(paste0(indice, "_2019_6"), paste0(indice, "_2019_7"), paste0(indice, "_2019_8"))
    # Otonio
    otonio_2018 <- c(paste0(indice, "_2018_9"), paste0(indice, "_2018_10"), paste0(indice, "_2018_11"))
    otonio_2019 <- c(paste0(indice, "_2019_9"), paste0(indice, "_2019_10"), paste0(indice, "_2019_11"))
    # Invierno
    invierno_2018 <- c(paste0(indice, "_2018_1"), paste0(indice, "_2018_2"))
    invierno_2019 <- c(paste0(indice, "_2019_1"), paste0(indice, "_2019_2"))
    # Primavera
    primavera_2018 <- c(paste0(indice, "_2018_3"), paste0(indice, "_2018_4"), paste0(indice, "_2018_5"))
    primavera_2019 <- c(paste0(indice, "_2019_3"), paste0(indice, "_2019_4"), paste0(indice, "_2019_5"))
    # Cico agricola
    #ciclo_2018 <- c(paste0(indice, "_2018_3"), paste0(indice, "_2018_4"), paste0(indice, "_2018_5"),
    #                paste0(indice, "_2018_6"), paste0(indice, "_2018_7"), paste0(indice, "_2018_8"))
    #ciclo_2019 <- c(paste0(indice, "_2019_3"), paste0(indice, "_2019_4"), paste0(indice, "_2019_5"),
    #                paste0(indice, "_2019_6"), paste0(indice, "_2019_7"), paste0(indice, "_2019_8"))
    # Cico agricola
    anual_2018 <- c(paste0(indice, "_2018_1"), paste0(indice, "_2018_2"), paste0(indice, "_2018_3"), 
                    paste0(indice, "_2018_4"), paste0(indice, "_2018_5"), paste0(indice, "_2018_6"), 
                    paste0(indice, "_2018_7"), paste0(indice, "_2018_8"), paste0(indice, "_2018_9"),
                    paste0(indice, "_2018_10"), paste0(indice, "_2018_11"), paste0(indice, "_2018_12"))
    
    anual_2019 <- c(paste0(indice, "_2019_1"), paste0(indice, "_2019_2"), paste0(indice, "_2019_3"), 
                    paste0(indice, "_2019_4"), paste0(indice, "_2019_5"), paste0(indice, "_2019_6"), 
                    paste0(indice, "_2019_7"), paste0(indice, "_2019_8"), paste0(indice, "_2019_9"),
                    paste0(indice, "_2019_10"), paste0(indice, "_2019_11"), paste0(indice, "_2019_12"))
    

    # # Julio
    # julio_2018 <- paste0(indice, "_2018_1")
    # julio_2019 <- paste0(indice, "_2019_1")
    # # Agosto
    # agosto_2018 <- paste0(indice, "_2018_2")
    # agosto_2019 <- paste0(indice, "_2019_2")
    # # Septiembre
    # septiembre_2018 <- paste0(indice, "_2018_3")
    # septiembre_2019 <- paste0(indice, "_2019_3")
    # # Octubre
    # octubre_2018 <- paste0(indice, "_2018_4")
    # octubre_2019 <- paste0(indice, "_2019_4")
    # # Noviembre
    # noviembre_2018 <- paste0(indice, "_2018_5")
    # noviembre_2019 <- paste0(indice, "_2019_5")
    # # Diciembre
    # diciembre_2018 <- paste0(indice, "_2018_6")
    # diciembre_2019 <- paste0(indice, "_2019_6")
    # # Enero
    # enero_2018 <- paste0(indice, "_2018_7")
    # enero_2019 <- paste0(indice, "_2019_7")
    # # Febrero
    # febrero_2018 <- paste0(indice, "_2018_8")
    # febrero_2019 <- paste0(indice, "_2019_8")
    # # Marzo
    # marzo_2018 <- paste0(indice, "_2018_9")
    # marzo_2019 <- paste0(indice, "_2019_9")
    # # Abril
    # abril_2018 <- paste0(indice, "_2018_10")
    # abril_2019 <- paste0(indice, "_2019_10")
    # # Mayo
    # mayo_2018 <- paste0(indice, "_2018_11")
    # mayo_2019 <- paste0(indice, "_2019_11")
    # # Junio
    # junio_2018 <- paste0(indice, "_2018_12")
    # junio_2019 <- paste0(indice, "_2019_12")
   
    
    dataset_auxiliar.i <- dataset_auxiliar %>%
      dplyr::select(Id, Cultivo, Elevacion, Dataset, Campania,
                    GlobalId, train, CultivoId, Tipo, 
                    starts_with(c(indice))) %>%
      mutate(
             # Verano
             verano_2018_sd = pmap_dbl(.[verano_2018], ~sd(c(...))),
             verano_2018_median = pmap_dbl(.[verano_2018], ~median(c(...))),
             verano_2019_sd = pmap_dbl(.[verano_2019], ~sd(c(...))),
             verano_2019_median = pmap_dbl(.[verano_2019], ~median(c(...))),
             # Otonio
             otonio_2018_sd = pmap_dbl(.[otonio_2018], ~sd(c(...))),
             otonio_2018_median = pmap_dbl(.[otonio_2018], ~median(c(...))),
             otonio_2019_sd = pmap_dbl(.[otonio_2019], ~sd(c(...))),
             otonio_2019_median = pmap_dbl(.[otonio_2019], ~median(c(...))),
             # Invierno
             invierno_2018_sd = pmap_dbl(.[invierno_2018], ~sd(c(...))),
             invierno_2018_median = pmap_dbl(.[invierno_2018], ~median(c(...))),
             invierno_2019_sd = pmap_dbl(.[invierno_2019], ~sd(c(...))),
             invierno_2019_median = pmap_dbl(.[invierno_2019], ~median(c(...))),
             # Primavera
             primavera_2018_sd = pmap_dbl(.[primavera_2018], ~sd(c(...))),
             primavera_2018_median = pmap_dbl(.[primavera_2018], ~median(c(...))),
             primavera_2019_sd = pmap_dbl(.[primavera_2019], ~sd(c(...))),
             primavera_2019_median = pmap_dbl(.[primavera_2019], ~median(c(...))),
             # # Ciclo agricola
             # ciclo_2018_sd = pmap_dbl(.[ciclo_2018], ~sd(c(...))),
             # ciclo_2018_max = pmap_dbl(.[ciclo_2018], ~max(c(...))),
             # ciclo_2019_sd = pmap_dbl(.[ciclo_2019], ~sd(c(...))),
             # ciclo_2019_max = pmap_dbl(.[ciclo_2019], ~max(c(...))),
             # Anual
             anual_2018_sd = pmap_dbl(.[anual_2018], ~sd(c(...))),
             anual_2018_max = pmap_dbl(.[anual_2018], ~max(c(...))),
             anual_2018_min = pmap_dbl(.[anual_2018], ~min(c(...))),
             anual_2018_median = pmap_dbl(.[anual_2018], ~median(c(...))),
             max_ind_2018 = pmap_int(.[anual_2018], ~which.max(c(...))),
             anual_2019_sd = pmap_dbl(.[anual_2019], ~sd(c(...))),
             anual_2019_max = pmap_dbl(.[anual_2019], ~max(c(...))),
             max_ind_2019 = pmap_int(.[anual_2019], ~which.max(c(...))),
             anual_2019_min = pmap_dbl(.[anual_2019], ~min(c(...))),
             anual_2019_median = pmap_dbl(.[anual_2019], ~median(c(...)))) %>%
             # Diferencias
      #        lag_julio_2018 = UQ(rlang::sym(julio_2018))  - 0,
      #        lag_agosto_2018 = UQ(rlang::sym(agosto_2018)) - UQ(rlang::sym(julio_2018)),
      #        lag_septiembre_2018 = UQ(rlang::sym(septiembre_2018)) - UQ(rlang::sym(agosto_2018)),
      #        lag_octubre_2018 = UQ(rlang::sym(octubre_2018)) - UQ(rlang::sym(septiembre_2018)),
      #        lag_noviembre_2018 = UQ(rlang::sym(noviembre_2018)) - UQ(rlang::sym(octubre_2018)),
      #        lag_diciembre_2018 = UQ(rlang::sym(diciembre_2018)) - UQ(rlang::sym(noviembre_2018)),
      #        lag_enero_2018 = UQ(rlang::sym(enero_2018)) - UQ(rlang::sym(diciembre_2018)),
      #        lag_febrero_2018 = UQ(rlang::sym(febrero_2018)) - UQ(rlang::sym(enero_2018)),
      #        lag_marzo_2018 = UQ(rlang::sym(marzo_2018)) - UQ(rlang::sym(febrero_2018)),
      #        lag_abril_2018 = UQ(rlang::sym(abril_2018)) - UQ(rlang::sym(marzo_2018)),
      #        lag_mayo_2018 = UQ(rlang::sym(mayo_2018)) - UQ(rlang::sym(abril_2018)),
      #        lag_junio_2018 = UQ(rlang::sym(junio_2018)) - UQ(rlang::sym(mayo_2018)),
      #        lag_julio_2019 = UQ(rlang::sym(julio_2019)) - UQ(rlang::sym(junio_2018)),
      #        lag_agosto_2019 = UQ(rlang::sym(agosto_2019)) - UQ(rlang::sym(julio_2019)),
      #        lag_septiembre_2019 = UQ(rlang::sym(septiembre_2019)) - UQ(rlang::sym(agosto_2019)),
      #        lag_octubre_2019 = UQ(rlang::sym(octubre_2019)) - UQ(rlang::sym(septiembre_2019)),
      #        lag_noviembre_2019 = UQ(rlang::sym(noviembre_2019)) - UQ(rlang::sym(octubre_2019)),
      #        lag_diciembre_2019 = UQ(rlang::sym(diciembre_2019)) - UQ(rlang::sym(noviembre_2019)),
      #        lag_enero_2019 = UQ(rlang::sym(enero_2019)) - UQ(rlang::sym(diciembre_2019)),
      #        lag_febrero_2019 = UQ(rlang::sym(febrero_2019)) - UQ(rlang::sym(enero_2019)),
      #        lag_marzo_2019 = UQ(rlang::sym(marzo_2019)) - UQ(rlang::sym(febrero_2019)),
      #        lag_abril_2019 = UQ(rlang::sym(abril_2019)) - UQ(rlang::sym(marzo_2019)),
      #        lag_mayo_2019 = UQ(rlang::sym(mayo_2019)) - UQ(rlang::sym(abril_2019)),
      #        lag_junio_2019 = UQ(rlang::sym(junio_2019)) - UQ(rlang::sym(mayo_2019))) %>%
      # # Combinar anos en una misma variable
      dplyr::mutate(# Verano
                     verano_sd = if_else(Campania == 2018, verano_2018_sd, verano_2019_sd),
                     verano_median = if_else(Campania == 2018, verano_2018_median, verano_2019_median),
                     # Otonio
                     otonio_sd = if_else(Campania == 2018, otonio_2018_sd , otonio_2019_sd),
                     otonio_median = if_else(Campania == 2018, otonio_2018_median, otonio_2019_median),
                     # Invierno
                     invierno_sd = if_else(Campania == 2018, invierno_2018_sd , invierno_2019_sd),
                     invierno_median = if_else(Campania == 2018, invierno_2018_median, invierno_2019_median),
                     # Primavera
                     primavera_sd = if_else(Campania == 2018, primavera_2018_sd , primavera_2019_sd),
                     primavera_median = if_else(Campania == 2018, primavera_2018_median, primavera_2019_median),
      #               # Ciclo
      #               ciclo_sd = if_else(Campania == 2018, ciclo_2018_sd, ciclo_2019_sd),
      #               ciclo_max = if_else(Campania == 2018, ciclo_2018_max, ciclo_2019_max),
      #               # Anual 
                      anual_sd = if_else(Campania == 2018, anual_2018_sd, anual_2019_sd),
                      anual_max = if_else(Campania == 2018, anual_2018_max, anual_2019_max),
                      anual_min = if_else(Campania == 2018, anual_2018_min, anual_2019_min),
                      anual_median = if_else(Campania == 2018, anual_2018_median, anual_2019_median),
                      anual_max_ind = if_else(Campania == 2018, max_ind_2018, max_ind_2019)) %>%
      #               # Meses
      #               julio = if_else(Campania == 2018, julio_2018, julio_2019),
      #               agosto = if_else(Campania == 2018, agosto_2018, agosto_2019),
      #               septiembre = if_else(Campania == 2018, septiembre_2018,septiembre_2019),
      #               octubre = if_else(Campania == 2018, octubre_2018, octubre_2019),
      #               noviembre = if_else(Campania == 2018, noviembre_2018, noviembre_2019),
      #               diciembre = if_else(Campania == 2018, diciembre_2018,diciembre_2019),
      #               enero = if_else(Campania == 2018, enero_2018, enero_2019),
      #               febrero = if_else(Campania == 2018, febrero_2018, febrero_2019),
      #               marzo = if_else(Campania == 2018, marzo_2018, marzo_2019),
      #               abril = if_else(Campania == 2018, abril_2018, abril_2019),
      #               mayo = if_else(Campania == 2018, mayo_2018, mayo_2019),
      #               junio = if_else(Campania == 2018, junio_2018, junio_2019),
      #               # Meses lags
      #               lag.julio = if_else(Campania == 2018,lag_julio_2018, lag_julio_2019),
      #               lag.agosto = if_else(Campania == 2018,lag_agosto_2018, lag_agosto_2019),
      #               lag.septiembre = if_else(Campania == 2018, lag_septiembre_2018, lag_septiembre_2019),
      #               lag.octubre = if_else(Campania == 2018, lag_octubre_2018, lag_octubre_2019),
      #               lag.noviembre = if_else(Campania == 2018, lag_noviembre_2018, lag_noviembre_2019),
      #               lag.diciembre = if_else(Campania == 2018, lag_diciembre_2018, lag_diciembre_2019),
      #               lag.enero = if_else(Campania == 2018, lag_enero_2018, lag_enero_2019),
      #               lag.febrero = if_else(Campania == 2018, lag_febrero_2018, lag_febrero_2019),
      #               lag.marzo = if_else(Campania == 2018, lag_marzo_2018, lag_marzo_2019),
      #               lag.abril = if_else(Campania == 2018, lag_abril_2018, lag_abril_2019),
      #               lag.mayo = if_else(Campania == 2018, lag_mayo_2018, lag_mayo_2019),
      #               lag.junio = if_else(Campania == 2018,lag_junio_2018, lag_junio_2019)) %>%
      # dplyr::select(!contains('lag_')) %>%
      #dplyr::select(Tipo, verano_mean, verano_max, verano_sd) %>%
      rename_at(vars("verano_sd"), funs(paste0(indice,"_verano_sd"))) %>%
      rename_at(vars("verano_median"), funs(paste0(indice,"_verano_median"))) %>%
      rename_at(vars("otonio_median"), funs(paste0(indice,"_otonio_median"))) %>%
      rename_at(vars("otonio_sd"), funs(paste0(indice,"_otonio_sd"))) %>%
      rename_at(vars("primavera_median"), funs(paste0(indice,"_primavera_median"))) %>%
      rename_at(vars("primavera_sd"), funs(paste0(indice,"_primavera_sd"))) %>%
      rename_at(vars("invierno_median"), funs(paste0(indice,"_invierno_median"))) %>%
      rename_at(vars("invierno_sd"), funs(paste0(indice,"_invierno_sd"))) %>%
      
      #rename_at(vars("ciclo_sd"), funs(paste0(indice,"_ciclo_sd"))) %>%
      #rename_at(vars("ciclo_max"), funs(paste0(indice,"_ciclo_max"))) %>%
      #Anual 
      rename_at(vars("anual_median"), funs(paste0(indice,"_anual_median"))) %>%
      rename_at(vars("anual_sd"), funs(paste0(indice,"_anual_sd"))) %>%
      rename_at(vars("anual_max"), funs(paste0(indice,"_anual_max"))) %>%
      rename_at(vars("anual_min"), funs(paste0(indice,"_anual_min"))) %>%
      rename_at(vars("anual_max_ind"), funs(paste0(indice,"_anual_max_ind"))) %>%

      # Meses lag
      # rename_at(vars("lag.julio"), funs(paste0(indice,"_lag_julio"))) %>%
      # rename_at(vars("lag.agosto"), funs(paste0(indice,"_lag_agosto"))) %>%
      # rename_at(vars("lag.septiembre"), funs(paste0(indice,"_lag_septiembre"))) %>%
      # rename_at(vars("lag.octubre"), funs(paste0(indice,"_lag_octubre"))) %>%
      # rename_at(vars("lag.noviembre"), funs(paste0(indice,"_lag_noviembre"))) %>%
      # rename_at(vars("lag.diciembre"), funs(paste0(indice,"_lag_diciembre"))) %>%
      # rename_at(vars("lag.enero"), funs(paste0(indice,"_lag_enero"))) %>%
      # rename_at(vars("lag.febrero"), funs(paste0(indice,"_lag_febrero"))) %>%
      # rename_at(vars("lag.marzo"), funs(paste0(indice,"_lag_marzo"))) %>%
      # rename_at(vars("lag.abril"), funs(paste0(indice,"_lag_abril"))) %>%
      # rename_at(vars("lag.mayo"), funs(paste0(indice,"_lag_mayo"))) %>%
      # rename_at(vars("lag.junio"), funs(paste0(indice,"_lag_junio"))) %>%
      
      # Meses 
      # rename_at(vars("julio"), funs(paste0(indice,"_julio"))) %>%
      # rename_at(vars("agosto"), funs(paste0(indice,"_agosto"))) %>%
      # rename_at(vars("septiembre"), funs(paste0(indice,"_septiembre"))) %>%
      # rename_at(vars("octubre"), funs(paste0(indice,"_octubre"))) %>%
      # rename_at(vars("noviembre"), funs(paste0(indice,"_noviembre"))) %>%
      # rename_at(vars("diciembre"), funs(paste0(indice,"_diciembre"))) %>%
      # rename_at(vars("enero"), funs(paste0(indice,"_enero"))) %>%
      # rename_at(vars("febrero"), funs(paste0(indice,"_febrero"))) %>%
      # rename_at(vars("marzo"), funs(paste0(indice,"_marzo"))) %>%
      # rename_at(vars("abril"), funs(paste0(indice,"_abril"))) %>%
      # rename_at(vars("mayo"), funs(paste0(indice,"_mayo"))) %>%
      # rename_at(vars("junio"), funs(paste0(indice,"_junio"))) %>%
         dplyr::select(contains(c('_verano_', '_primavera_', '_anual_',
                                  '_otonio_', '_invierno_')))
    
  }
)



# Dataset completo
write.csv(dataset_auxiliar, 
          file = glue::glue("{config$dir$input}/dataset_satelite_resumen_bandas.csv"),
          row.names = F)

indices <- c("GNDVI", "SAVI", "MSI", "BSI")

# Dataset solo con cuatro indices y es el usado en el entrenamiento del modelo
write.csv(dataset_auxiliar %>%
            dplyr::select(Id, Cultivo, Elevacion, Dataset, Campania,
                          GlobalId, train, CultivoId, Tipo) %>%
            cbind(dataset_resumen %>%
              dplyr::select(contains(indices))), 
          file = glue::glue("{config$dir$input}/dataset_satelite_resumen_anual_estacional.csv"),
          row.names = F)


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
               "caret", "xgboost", "superml", "yardstick")

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
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 5: Manipulacion de imagenes ----
# -----------------------------------------------------------------------------#
som <- function(x) {
  as.Date(format(x, "%Y-%m-01"))
}

eom <- function(x) {
  som(som(x) + 35) - 1
}

# No te olvides del año agricila 
anio_agr <- function(dates, start_month=7) {
  # Convert dates into POSIXlt
  dates.posix = as.POSIXlt(dates)
  # Year offset
  offset = ifelse(dates.posix$mon >= start_month - 1, 1, 0)
  # Agri year
  adj.year = dates.posix$year + 1900 + offset - 1
  # Return the agri year
  adj.year
}

fechas.procesables <- data.frame(date =
                                   seq(from = as.Date(config$params$gge$date$begin_date),
                                       to = as.Date(config$params$gge$date$end_date),
                                       by = 'days')) %>%
  dplyr::mutate(month = lubridate::month(date),
                year = lubridate::year(date)) %>%
  dplyr::group_by(year, month) %>%
  dplyr::summarise() %>%
  dplyr::ungroup(.) %>%
  dplyr::mutate(date = lubridate::ymd(paste(year, month, 15L))) %>%
  dplyr::mutate(begin_month = som(date),
                end_month = eom(date),
                anio_agricola = anio_agr(date, 8)) %>%
  dplyr::filter(anio_agricola < 2020)

# Si el directorio de trabajo existe, listar los archivos descargados
if (fs::dir_exists(glue::glue("{config$dir$input}/images/"))) {
  
  list.files <-  purrr::map_chr(
    .x = 1:nrow(fechas.procesables),
    .f = function(imagen) {
      
      fecha_i <- fechas.procesables[imagen,]
      
      # Nombre del archivo raster
      if (config$params$gge$coleccion == 'COPERNICUS/S2') {
        coleccion = 'sentinel'
      } else {
        coleccion = 'landsat'
      }
      
      file.name <- glue::glue("{config$dir$input}/images/imagen_{coleccion}_{fecha_i$year}_{fecha_i$month}.tif")
      
    }
  )
}

# Extraer los datos para cada punto relevado
rmRaster <- function(x, verbose = FALSE) {
  
  stopifnot(grepl("Raster",class(x)))
  ## get raster temporary directory
  sink("aux")
  tdir=rasterOptions()[["tmpdir"]]
  sink(NULL)
  
  has.tmp <- fromDisk(x) & file.exists(file.path(tdir, basename(x@file@name))) 
  if (has.tmp) file.remove(x@file@name, sub('grd', 'gri', x@file@name))
  
  parent.var.name <- deparse(substitute(x))
  rm(list = parent.var.name, envir = sys.frame(-1))
  
  if (verbose) print(paste('Removed: ', parent.var.name))
  if (verbose & has.tmp) {
    print(paste('Deleted: ', x@file@name))
    print(paste('Deleted: ', sub('grd', 'gri', x@file@name)))
  }
}

# No te olvides de corregir
dataset_satelite <- purrr::map_dfc(
  .x = 1:nrow(fechas.procesables),
  .f = function(imagen) {
    
    # Fecha a procesar
    fecha_i <- fechas.procesables[imagen,]
    
    # Bandas a utilizar
    bandas <- config$params$gge$bandas
    
    # Leer el stack correspondiente
    datos_fecha.i <- raster::stack(list.files[imagen]) %>%
      #raster::calc(., fun = function(x) {x / 10000}) %>%
      setNames(., bandas) 
    
    # Máscara de nubes
    # Banda 2:Azul
    #banda_2 <- datos_fecha.i$B2

    # Matriz de clasificacion
    class.m <- c(1500, Inf, NA,
                 -Inf, 1500, 1)
    
    rcl.m <- matrix(class.m, 
                    ncol=3, 
                    byrow=TRUE)
    
    banda_2_mask <- reclassify(datos_fecha.i$B2, 
                         rcl.m)
    # Banda 10: Cirrus
    class.m <- c(50, Inf, NA,
                 -Inf, 50, 1)
    
    rcl.m <- matrix(class.m, 
                    ncol=3, 
                    byrow=TRUE)
    
    banda_10_mask <- reclassify(datos_fecha.i$B10, 
                          rcl.m)
    
    # Banda 11: SWIR
    #banda_11 <- datos_fecha.i$B11
    class.m <- c(4000, Inf, NA,
                 -Inf, 4000, 1)
    
    rcl.m <- matrix(class.m, 
                    ncol=3, 
                    byrow=TRUE)
    
    banda_11_mask <- reclassify(datos_fecha.i$B11, 
                                rcl.m)

    # Mascara 
    mask <- overlay(banda_2_mask, banda_11_mask, banda_11_mask, fun=function(x,y,z) {(x*y*z)} )
    datos_fecha.i <- overlay(datos_fecha.i, mask, fun=function(x,y){(x*y)} ) %>%
      setNames(., bandas) 
    # Enmascarar resultados
    #datos_fecha.i_masked <- datos_fecha.i * banda_2 * banda_11
    #datos_fecha.i_masked <- setNames(datos_fecha.i_masked, bandas) 
    
    # Extraer datos para los puntos
    datos_fecha._matrix.i <- datos_fecha.i %>%
      raster::extract(., dataset)
    
    dataset.i <- dataset %>%
      sf::st_set_geometry(NULL) %>%
      cbind(datos_fecha._matrix.i/10000) %>%
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
        
      )
    
    # fecha actual de preocsamiento
    fecha <- glue::glue("_{fecha_i$year}_{fecha_i$month}")
    # Anio agricola correspondiente
    campana <- anio_agr(as.Date(glue::glue("{fecha_i$year}-{fecha_i$month}-15")), 7)
    #mes_campania <- lubridate::quarter(as.Date(glue::glue("{fecha_i$year}-{fecha_i$month}-15")), fiscal_start = 8)
    # Correccion de fecha por anio agricola
    fecha <- glue::glue("_{fecha_i$year}_{fecha_i$month}")
    # indices usados
    indices <- c("NGRDI", "TGI", "NDVI", "GNDVI", "EVI", "AVI", 
                 "SAVI", "NDWI", "MSI", "BSI", "MCARI", "RED", 'NDRE', "NDII")
    indices_fecha <- stringr::str_c(indices, fecha)
    # bandas usadas
    bandas_fecha <- stringr::str_c(bandas, fecha)
    
    dataset.i <- dataset.i %>%
      dplyr::rename_at(vars(bandas), ~bandas_fecha) %>%
      dplyr::rename_at(vars(indices), ~indices_fecha) %>%
      mutate_at(vars(bandas_fecha), list(~ ifelse(Campania == campana, ., 0))) %>%
      mutate_at(vars(indices_fecha), list(~ ifelse(Campania == campana, ., 0))) %>%
      dplyr::select(bandas_fecha, indices_fecha)

    cat('Completada: ', fecha)
    
    rmRaster(banda_2_mask)
    rmRaster(banda_10_mask)
    rmRaster(banda_11_mask)
    rmRaster(datos_fecha.i)
    
    return(dataset.i)
    
  }
)


# Guardar dataset 
save(dataset_satelite, file = glue::glue("{config$dir$input}/dataset_satelite_observado.RData"))
# -----------------------------------------------------------------------------






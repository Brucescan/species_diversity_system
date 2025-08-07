// =================================================================================
// 1. 定义参数
// =================================================================================

// 你的矢量文件Table ID
var table = ee.FeatureCollection('projects/ee-brucepengyuan/assets/bei_jing');
// 获取矢量文件的矩形范围 (Bounding Box)
var aoi = table.geometry().bounds();
// 定义时间范围
var startDate = '2025-01-01';
var endDate = '2025-7-31';
// 定义导出到Google Drive的文件夹名称
var outputFolder = 'GEE_TerraClimate_Data';
// 定义你需要的最终分辨率 (2000米)
var targetScale = 2000;

// =================================================================================
// 2. 加载和筛选 TerraClimate 数据
// =================================================================================

// 加载TerraClimate月度数据集
var terraClimate = ee.ImageCollection('IDAHO_EPSCOR/TERRACLIMATE')
  .filterBounds(aoi)
  .filterDate(startDate, endDate)
  // 选择我们需要的原始波段
  .select([
    'tmmx', // 月最高气温 (degC * 10)
    'tmmn', // 月最低气温 (degC * 10)
    'pr',   // 月总降水量 (mm)
    'vs',   // 10米风速 (m/s * 100) <-- 【已修正】波段名称从 'ws' 改为 'vs'
    'vap'   // 蒸汽压 (kPa * 1000) - 作为湿度代理
  ]);

// 打印检查筛选后的影像集合
print('Filtered TerraClimate Collection:', terraClimate);

// =================================================================================
// 3. 数据预处理函数 (计算均温、应用比例因子)
// =================================================================================

var preprocessTerraClimate = function(image) {
  // --- 提取原始波段 ---
  var tmmx = image.select('tmmx');
  var tmmn = image.select('tmmn');
  var pr = image.select('pr');
  var vs = image.select('vs');   // <-- 【已修正】
  var vap = image.select('vap');

  // --- 变量计算和单位校正 ---
  // 1. 计算月平均气温 (℃)。比例因子 0.1
  var mean_temp = tmmx.add(tmmn).divide(2).multiply(0.1)
                      .rename('temperature_C');

  // 2. 降水量单位已是mm，只需重命名。
  var precip_mm = pr.rename('precipitation_mm');

  // 3. 风速(m/s)。根据文档，比例因子为 0.01。
  var wind_speed = vs.multiply(0.01).rename('wind_speed_ms'); // <-- 【已修正】应用了0.01的比例因子

  // 4. 蒸汽压(kPa)。根据文档，比例因子为 0.001。
  var vapor_pressure = vap.multiply(0.001)
                         .rename('vapor_pressure_kPa');

  // 将处理后的波段合并成一个新的影像，并保留原始影像的时间戳
  return ee.Image.cat([
    mean_temp,
    precip_mm,
    wind_speed,
    vapor_pressure
  ]).copyProperties(image, ['system:time_start']);
};

// 使用 map() 函数将预处理应用到集合中的每一张影像
var processedCollection = terraClimate.map(preprocessTerraClimate);

// 打印检查处理后的影像集合
print('Processed Collection:', processedCollection);

// =================================================================================
// 4. 可视化检查（可选，但推荐）
// =================================================================================

Map.centerObject(aoi, 7);
var tempVisParam = {
  min: -20,
  max: 30,
  palette: ['blue', 'cyan', 'yellow', 'red']
};
var firstImage = processedCollection.first();
Map.addLayer(firstImage.select('temperature_C'), tempVisParam, 'Mean Temperature (°C)');
Map.addLayer(aoi, {color: 'FF0000', fillColor: '00000000'}, 'Beijing Bounding Box');

// =================================================================================
// 5. 导出数据到 Google Drive (包含重采样和正确投影)
// =================================================================================

// 同样，建议使用分批次导出的方法来避免客户端循环超时
// 您可以手动修改年份 '2020', '2021', '2022', '2023', '2024' 来分批运行
var year_to_export = '2024';

var collection_for_year = processedCollection.filter(
  ee.Filter.calendarRange(parseInt(year_to_export), parseInt(year_to_export), 'year')
);

var collectionList = collection_for_year.toList(collection_for_year.size());
var listSize = collectionList.size().getInfo();

for (var i = 0; i < listSize; i++) {
  var image = ee.Image(collectionList.get(i));
  var date = ee.Date(image.get('system:time_start')).format('YYYY_MM');
  var dateStr = date.getInfo();

  var fileName = 'TerraClimate_Beijing_2km_UTM50N_' + dateStr;
  var taskDescription = 'Export_TC_Beijing_2km_UTM50N_' + dateStr;

  Export.image.toDrive({
    image: image.toFloat(),
    description: taskDescription,
    folder: outputFolder,
    fileNamePrefix: fileName,
    region: aoi,
    scale: targetScale, // 2000
    crs: 'EPSG:32650', // WGS 1984 UTM Zone 50N
    maxPixels: 1e13
  });
}
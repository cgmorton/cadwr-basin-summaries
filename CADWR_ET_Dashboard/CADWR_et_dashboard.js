// Last Updated: 09/12/2026 by blake minor
// Path: users/bminor-dri/CADWR/ET_EEapp/et_aggregations_charting_download_app

/*******************************************************************************
 * Model *
 ******************************************************************************/

// 2023-2025 CADWR County, groundwater basin, and hydrologic region ET
// variables:
// ET, Pixel Count, Missing ET Flag



// Define a JSON object for storing the data model.
var text = require('users/gena/packages:text');
var palettes = require('users/gena/packages:palettes');

var m = {};

m.aggType = ['COUNTY', 'GW_BASIN', 'HYDRO_REGION'];

m.modelName = ["ENSEMBLE", "EEMETRIC", "DISALEXI", "GEESEBAL", "PTJPL", "SIMS", "SSEBOP"];

m.landCover = ["AG_LANDS", "ALL_LANDS"];

// Get final year of system
m.yearEnd = ee.Number(2025);
m.yearList = ee.List.sequence(2003, m.yearEnd, 1).map(function(year) {
    return ee.String(ee.Number(year).int());
}).getInfo();

// CONUS thumbnail
// m.thumbnail_img = ee.Image('projects/bor-evap/assets/bor-ee-app/fc-database/west-conus-outline-image');



// --------------------VISUALIZATION----------------------------------------

// state border
var state = ee.FeatureCollection("TIGER/2018/States")
  .filter(ee.Filter.eq('NAME', 'California'));
  
// agricultural mask
var ag_mask = ee.Image('projects/csumb-et-tools/assets/ca2024_urbanmask');
ag_mask = ag_mask.updateMask(ag_mask.neq(82));


var modelOrder = [
  'ENSEMBLE',
  'EEMETRIC',
  'DISALEXI',
  'GEESEBAL',
  'PTJPL',
  'SIMS',
  'SSEBOP'
];

var color_dict = {
  'ENSEMBLE': 'navy',
  'EEMETRIC': 'lightblue',
  'DISALEXI': 'purple',
  'GEESEBAL': 'magenta',
  'PTJPL': 'limegreen',
  'SIMS': 'yellow',
  'SSEBOP': 'orange',
};

// color_dict must be a normal JavaScript object, not ee.Dictionary.
var color_list = modelOrder.map(function(modelName) {
  return color_dict[modelName];
});


// -------------------------------------------------------------------------


 


// -----------------------------------DATA-----------------------------------


// #########  data tables used for charting

// 2003-2025 monthly ET stats data tables
m.monthly_ag_data_tab_county = ee.FeatureCollection('projects/ee-bminor/assets/CDWR/ET_EEapp/county_ag_lands_all_models');
m.monthly_all_data_tab_county = ee.FeatureCollection('projects/ee-bminor/assets/CDWR/ET_EEapp/county_all_lands_all_models');
m.monthly_ag_data_tab_gw_basin = ee.FeatureCollection('projects/ee-bminor/assets/CDWR/ET_EEapp/gw_basin_ag_lands_all_models');
m.monthly_all_data_tab_gw_basin = ee.FeatureCollection('projects/ee-bminor/assets/CDWR/ET_EEapp/gw_basin_all_lands_all_models');
m.monthly_ag_data_tab_hr_region = ee.FeatureCollection('projects/ee-bminor/assets/CDWR/ET_EEapp/hydro_region_ag_lands_all_models');
m.monthly_all_data_tab_hr_region = ee.FeatureCollection('projects/ee-bminor/assets/CDWR/ET_EEapp/hydro_region_all_lands_all_models');
m.all_data_tab = m.monthly_ag_data_tab_county.merge(m.monthly_all_data_tab_county)
  .merge(m.monthly_ag_data_tab_gw_basin).merge(m.monthly_all_data_tab_gw_basin)
  .merge(m.monthly_ag_data_tab_hr_region).merge(m.monthly_all_data_tab_hr_region);


// ############ feature collections used for initial list of COUNTY/GW_BASIN/HYDRO_REGION ID's and filtering bounds onClick ###########

// 2003-2025 ET stats feature collections and feature views
// full dictionary with both the FC and FV IDs
m.f_dict = {
  'COUNTY': {fc: 'projects/ee-bminor/assets/CDWR/ET_EEapp/county_fc', fv: 'projects/ee-bminor/assets/CDWR/ET_EEapp/county_fv'},
  'GW_BASIN': {fc: 'projects/ee-bminor/assets/CDWR/ET_EEapp/gw_basin_fc', fv: 'projects/ee-bminor/assets/CDWR/ET_EEapp/gw_basin_fv'},
  'HYDRO_REGION': {fc: 'projects/ee-bminor/assets/CDWR/ET_EEapp/hr_region_fc', fv: 'projects/ee-bminor/assets/CDWR/ET_EEapp/hr_region_fv'},
};


// --------------------------------------------------------------------------




// Get list of initial aggregation ID's
m.aggNames = ee.FeatureCollection(m.f_dict.HYDRO_REGION.fc)
  .aggregate_array('AGG_ID').distinct().sort().getInfo();


// Add grey backgroung
m.background_map = [{ // Dial down the map saturation.
    stylers: [{
        saturation: -100
    }, {
        lightness: -30
    }]
}, { // Dial down the label darkness.
    elementType: 'labels',
    stylers: [{
        lightness: 30
    }]
}, { // Simplify the road geometries.
    featureType: 'road',
    elementType: 'geometry',
    stylers: [{
        visibility: 'simplified'
    }]
}, { // Turn off road labels.
    featureType: 'road',
    elementType: 'labels',
    stylers: [{
        visibility: 'off'
    }]
}, { // Turn off all icons.
    elementType: 'labels.icon',
    stylers: [{
        visibility: 'off'
    }]
}, { // Turn off all POIs.
    featureType: 'poi',
    elementType: 'all',
    stylers: [{
        visibility: 'off'
    }]
}];

// date format for charts
m.date_format = 'MMM-yyyy';



/*******************************************************************************
 * Components *
 *
 * A section to define the widgets that will compose your app.
 *
 * Guidelines:
 * 1. Except for static text and constraints, accept default values;
 *    initialize others in the initialization section.
 * 2. Limit composition of widgets to those belonging to an inseparable unit
 *    (i.e. a group of widgets that would make no sense out of order).
 ******************************************************************************/

// Define a JSON object for storing UI components.
var c = {};

c.controlPanel = ui.Panel();
c.mapPanel = ui.Map();

// On map info panel
c.map_info = {};
c.map_info.label = ui.Label('Click on or Select a Feature');
c.map_info.panel = ui.Panel([
    c.map_info.label
]);

// Control Panel pieces
c.info = {};
c.info.titleLabel = ui.Label("CADWR Historical ET");
c.info.panel = ui.Panel([
    c.info.titleLabel
]);

c.landCover = {};
c.landCover.title = ui.Label("Landcover Type");
c.landCover.landCoverSelector = ui.Select(m.landCover, "Select a Landcover", 'AG_LANDS');

c.modelName = {};
c.modelName.title = ui.Label("Model");
c.modelName.modelNameSelector = ui.Select(m.modelName, "Select a Model", 'ENSEMBLE');

c.aggName = {};
c.aggName.title = ui.Label("Select Feature");
c.aggName.Selector = ui.Select(m.aggNames, "Select a Feature");

c.aggType = {};
c.aggType.title = ui.Label("Aggregation Type");
c.aggType.aggTypeSelector = ui.Select({
  items: Object.keys(m.f_dict),
  placeholder: 'Select an Aggregation Type',
  onChange: function(selected) {
    
    var data = m.f_dict[selected];
  
    c.aggName.Selector.items().reset(ee.FeatureCollection(data.fc).aggregate_array('AGG_ID').distinct().sort().getInfo());
  
    // Replace FeatureView at index 1 in your mapPanel
    c.mapPanel.layers().set(1, ui.Map.FeatureViewLayer(data.fv, {
      fillColor: "purple",
      width: 0.9,
      opacity: 0.4,
      // lineWidth: 2,
    }, "ET Aggregation Boundary").setShown(true));
        
  }
    });


c.agg_type_panel = ui.Panel([
        ui.Panel([c.aggType.title, c.aggType.aggTypeSelector]),
        ui.Panel([c.aggName.title, c.aggName.Selector])
  ],
  ui.Panel.Layout.flow("horizontal")
);

c.model_name_panel = ui.Panel([
        ui.Panel([c.modelName.title, c.modelName.modelNameSelector]),
        ui.Panel([c.landCover.title, c.landCover.landCoverSelector])
    ],
    ui.Panel.Layout.flow("horizontal")
);

c.years = {};
c.years.StartSelector = ui.Select(m.yearList, "Select a Start Year");
c.years.startTitle = ui.Label("Start Year");
c.years.EndSelector = ui.Select(m.yearList, "Select an End Year");
c.years.endTitle = ui.Label("End Year");

c.years.panel = ui.Panel([
        ui.Panel([c.years.startTitle, c.years.StartSelector]),
        ui.Panel([c.years.endTitle, c.years.EndSelector])
    ],
    ui.Panel.Layout.flow("horizontal")
);


c.lat = ui.Label(37.5);
c.lon = ui.Label(-120);

// Check panel
c.checkbox = {};
c.checkbox.chart_0 = ui.Checkbox('Show ETa Intercomparison', true);
c.checkbox.chart_1 = ui.Checkbox('Show ETa Rates for selected model', true);
c.checkbox.chart_2 = ui.Checkbox('Show Pixel Count', true);
c.checkbox.chart_3 = ui.Checkbox('Show Missing ET Months', true);

c.checkbox.panel = ui.Panel([
        c.checkbox.chart_0, c.checkbox.chart_1, 
        c.checkbox.chart_2, c.checkbox.chart_3
    ],
    ui.Panel.Layout.flow('horizontal'));

// Chart Panels
c.charts = {};
c.charts.chart_0 = ui.Panel();
c.charts.chart_1 = ui.Panel();
c.charts.chart_2 = ui.Panel();
c.charts.chart_3 = ui.Panel();


c.charts.panel = ui.Panel([
    c.charts.chart_0, c.charts.chart_1, 
    c.charts.chart_2, c.charts.chart_3
]);


// Panel for table download
c.table_download = {};
c.table_download.titleLabel = ui.Label("Download Data");
c.table_download.panel = ui.Panel([
    c.table_download.titleLabel
]);

c.table_aggType = {};
c.table_aggType.title = ui.Label("Aggregation Type");
c.table_aggType.aggTypeSelector = ui.Select(m.aggType, "Select an Aggregation Type", 'HYDRO_REGION');

c.table_landCover = {};
c.table_landCover.title = ui.Label("Landcover Type");
c.table_landCover.landCoverSelector = ui.Select(m.landCover, "Select a Landcover", 'AG_LANDS');

c.table_modelName = {};
c.table_modelName.title = ui.Label("OpenET Model");
c.table_modelName.modelNameSelector = ui.Select(m.modelName, "Select a Model", 'ENSEMBLE');

c.download = {};
c.download.title = ui.Label("Download Data");
c.download.button = ui.Button({
    label: 'Update Link'
});

c.table_agg_type_panel = ui.Panel([
        ui.Panel([c.table_aggType.title, c.table_aggType.aggTypeSelector]),
        ui.Panel([c.download.title, c.download.button])
  ],
  ui.Panel.Layout.flow("horizontal")
);


c.table_model_name_panel = ui.Panel([
        ui.Panel([c.table_modelName.title, c.table_modelName.modelNameSelector]),
        ui.Panel([c.table_landCover.title, c.table_landCover.landCoverSelector]),  
    ],
    ui.Panel.Layout.flow("horizontal")
);

c.table_years = {};
c.table_years.StartSelector = ui.Select(m.yearList, "Select a Start Year");
c.table_years.startTitle = ui.Label("Start Year");
c.table_years.EndSelector = ui.Select(m.yearList, "Select an End Year");
c.table_years.endTitle = ui.Label("End Year");

c.table_years.panel = ui.Panel([
        ui.Panel([c.table_years.startTitle, c.table_years.StartSelector]),
        ui.Panel([c.table_years.endTitle, c.table_years.EndSelector])
    ],
    ui.Panel.Layout.flow("horizontal")
);

c.d_link = {};
c.d_link.d_label = ui.Label();
c.d_link.panel = ui.Panel();


// CADWR ET
c.dataVarsET = {};
c.dataVarsET.titleLabel = ui.Label("Data Variables:");
c.dataVarsET.text1 = ui.Label("ETa - OpenET actual ET");
c.dataVarsET.text2 = ui.Label("% of Max Pixels: values expressed as the percentage of max clear-sky pixel counts analyzed within the spatial aggregation, OpenET model, and landcover type. A value of 100% represents complete satellite retrieval coverage for the month, while lower values indicate that a smaller percentage of pixels were retrieved and produced valid ET estimates.");
c.dataVarsET.text3 = ui.Label("MISSING ET MONTH: Flag for when there were no monthly ET pixels available within the aggregation region due to cloud cover and/or a lack of Landsat overpass-date observations.");


c.dataVarsET.panel = ui.Panel([
    c.dataVarsET.titleLabel, c.dataVarsET.text1, c.dataVarsET.text2, c.dataVarsET.text3
]);


c.dataSources = {};
c.dataSources.titleLabel = ui.Label("Data Source:");
c.dataSources.text1 = ui.Label("CADWR GitHub Repo")
        .setUrl('https://github.com/cgmorton/cadwr-basin-summaries');


c.dataSources.panel = ui.Panel([
    c.dataSources.titleLabel, c.dataSources.text1,
]);

// Disclaimer Panel
c.disclaimer = {};

c.disclaimer.titleLabel = ui.Label("Data Disclaimer");
c.disclaimer.text = ui.Label("Data and information provided through this application are part of ongoing research and should be considered provisional and are subject to change. Users should perform thorough review prior to operational applications and decision making.");
c.disclaimer.panel = ui.Panel([
    c.disclaimer.titleLabel, c.disclaimer.text
]);


/*******************************************************************************
 * Composition *
 *
 * A section to compose the app i.e. add child widgets and widget groups to
 * first-level parent components like control panels and maps.
 *
 * Guidelines: There is a gradient between components and composition. There
 * are no hard guidelines here; use this section to help conceptually break up
 * the composition of complicated apps with many widgets and widget groups.
 ******************************************************************************/

ui.root.clear();
ui.root.add(c.controlPanel);
ui.root.add(c.mapPanel);

c.controlPanel.add(c.info.panel);

c.controlPanel.add(c.agg_type_panel);

c.controlPanel.add(c.model_name_panel);

c.controlPanel.add(c.years.panel);

c.mapPanel.add(c.map_info.panel);


c.mapPanel
  .layers()
  .set(0, ui.Map.Layer(ee.FeatureCollection(state).style({color:'black',fillColor:'#00000000'}), {}, 'State Border')
);

c.mapPanel
  .layers()
  .set(1, ui.Map.FeatureViewLayer(m.f_dict.HYDRO_REGION.fv, {}, "ET Aggregation Boundary")  
  .setVisParams({
    fillColor: "purple",
    width: 0.9,
    opacity: 0.4,
    // lineWidth: 2,
  })
  .setShown(1)
);

c.mapPanel
  .layers()
  .set(2, ui.Map.Layer(ee.Image(ag_mask), {min:0, max: 100, palette:['green'], opacity:0.7}, 'Agricultural Area Mask', false)
);

c.mapPanel.setOptions('Gray', {
    'Gray': m.background_map
});

c.mapPanel.onClick(function(coords) {

    // Update lat long values
    c.lat.setValue(coords.lat);
    c.lon.setValue(coords.lon);

    // Update Selector
    var point = ee.Geometry.Point([ee.Number.parse(c.lon.getValue()), ee.Number.parse(c.lat.getValue())]);
    var data = m.f_dict[c.aggType.aggTypeSelector.getValue()];
    var agg = ee.FeatureCollection(data.fc).filterBounds(point);

    // Check if there is a feature here
    var agg_check = agg.size().getInfo();

    // If no feature is Checked
    if (agg_check === 0) {
        c.map_info.panel.style().set({
            shown: true
        });
        c.map_info.label.setValue('Oops, you did not click on a valid feature. Try again!');

    }

    // If a feature is Selected
    if (agg_check > 0) {
        c.map_info.panel.style().set({
            shown: false
        });

        var agg_name = agg.first().get("AGG_ID").getInfo();

        c.aggName.Selector.setValue(agg_name);

        // add new layer to map
        highlight_fc();
        update_charts();
    }
});

c.controlPanel.add(c.checkbox.panel);

c.controlPanel.add(c.charts.panel);

c.controlPanel.add(c.table_download.panel);

c.controlPanel.add(c.table_agg_type_panel);

c.controlPanel.add(c.table_model_name_panel);

c.controlPanel.add(c.table_years.panel);

c.controlPanel.add(c.d_link.panel);

c.controlPanel.add(c.dataVarsET.panel);

c.controlPanel.add(c.dataSources.panel);

c.controlPanel.add(c.disclaimer.panel);

// c.mapPanel.add(c.thumbnail);

c.mapPanel.centerObject(ee.Geometry.Point([-120, 37.5]), 6.4);

/*******************************************************************************
 * Styling *
 *
 * A section to define and set widget style properties.
 *
 * Guidelines:
 * 1. At the top, define styles for widget "classes" i.e. styles that might be
 *    applied to several widgets, like text styles or margin styles.
 * 2. Set "inline" style properties for single-use styles.
 * 3. You can add multiple styles to widgets, add "inline" style followed by
 *    "class" styles. If multiple styles need to be set on the same widget, do
 *    it consecutively to maintain order.
 ******************************************************************************/

// Define a JSON object for defining CSS-like class style properties.
var s = {};

c.controlPanel.style().set({
    width: '800px',
    fontSize: '14px',
});
c.mapPanel.style().set({
    cursor: 'crosshair'
});


c.info.titleLabel.style().set({
    fontWeight: 'bold',
    fontSize: '26px',
    margin: '0 0 4px 0',
    padding: '20px'
});

c.aggType.aggTypeSelector.style().set({
    width: "200px",
    margin: '4px 4px 4px 40px',
});

c.aggType.title.style().set({
    fontSize: '14px',
    fontWeight: 'bold',
    margin: '4px 4px 4px 40px',
});

c.landCover.landCoverSelector.style().set({
    width: "200px",
    margin: '4px 4px 4px 40px',
});

c.landCover.title.style().set({
    fontSize: '14px',
    fontWeight: 'bold',
    margin: '4px 4px 4px 40px',
});

c.modelName.modelNameSelector.style().set({
    width: "200px",
    margin: '4px 4px 4px 40px',
});

c.modelName.title.style().set({
    fontSize: '14px',
    fontWeight: 'bold',
    margin: '4px 4px 4px 40px',
});

c.aggName.Selector.style().set({
    width: "200px",
    margin: '4px 4px 4px 40px',
});

c.aggName.title.style().set({
    fontSize: '14px',
    fontWeight: 'bold',
    margin: '4px 4px 4px 40px',
});

s.years = {};
s.years.StartSelector = {
    width: '200px',
    margin: '4px 4px 4px 40px',
};
s.years.EndSelector = {
    width: '200px',
    margin: '4px 4px 4px 40px',
};
s.years.startTitle = {
    fontSize: '14px',
    fontWeight: 'bold',
    margin: '4px 4px 4px 40px',
};
s.years.endTitle = {
    fontSize: '14px',
    fontWeight: 'bold',
    margin: '4px 4px 4px 40px',
};
s.years.panelStyle = {
    layout: ui.Panel.Layout.flow('horizontal')
};
c.years.StartSelector.style().set(s.years.StartSelector);
c.years.EndSelector.style().set(s.years.EndSelector);
c.years.startTitle.style().set(s.years.startTitle);
c.years.endTitle.style().set(s.years.endTitle);

s.HIGHLIGHT_STYLE = {
    color: 'white',
    fillColor: '00000000',
    width: 2,
    lineType: 'dashed'
};

s.textStyle = {
    color: 'black',
    fontName: 'arial',
    fontSize: 14,
    bold: true,
    italic: false
};

// Dowload table styling
c.table_download.titleLabel.style().set({
    fontWeight: 'bold',
    fontSize: '18px',
    margin: '0 0 4px 0',
    padding: '10px'
});

c.table_aggType.aggTypeSelector.style().set({
    width: "200px",
    margin: '4px 4px 4px 40px',
});

c.table_landCover.landCoverSelector.style().set({
    width: "200px",
    margin: '4px 4px 4px 40px',
});

c.table_modelName.modelNameSelector.style().set({
    width: "200px",
    margin: '4px 4px 4px 40px',
});

c.download.button.style().set({
    width: "200px",
    margin: '4px 4px 4px 40px',
});

c.table_aggType.title.style().set({
    fontSize: '14px',
    fontWeight: 'bold',
    margin: '4px 4px 4px 40px',
});

c.table_landCover.title.style().set({
    fontSize: '14px',
    fontWeight: 'bold',
    margin: '4px 4px 4px 40px',
});

c.table_modelName.title.style().set({
    fontSize: '14px',
    fontWeight: 'bold',
    margin: '4px 4px 4px 40px',
});

c.download.title.style().set({
    fontSize: '14px',
    fontWeight: 'bold',
    margin: '4px 4px 4px 40px',
});

s.table_years = {};
s.table_years.StartSelector = {
    width: '200px',
    margin: '4px 4px 4px 40px',
};
s.table_years.EndSelector = {
    width: '200px',
    margin: '4px 4px 4px 40px',
};

s.table_years.startTitle = {
    fontSize: '14px',
    fontWeight: 'bold',
    margin: '4px 4px 4px 40px',
};
s.table_years.endTitle = {
    fontSize: '14px',
    fontWeight: 'bold',
    margin: '4px 4px 4px 40px',
};
s.table_years.panelStyle = {
    layout: ui.Panel.Layout.flow('horizontal'),
};
c.table_years.StartSelector.style().set(s.table_years.StartSelector);
c.table_years.EndSelector.style().set(s.table_years.EndSelector);
c.table_years.startTitle.style().set(s.table_years.startTitle);
c.table_years.endTitle.style().set(s.table_years.endTitle);
c.d_link.d_label.style().set({
    fontSize: '20px'
});

// Set style for thumbnail
// s.thumbnail = {
//     height: '30px',
//     width: '30px',
//     padding: '2px',
//     position: 'top-left'
// };
// c.thumbnail.style().set(s.thumbnail);


// ET Data Variables
c.dataVarsET.titleLabel.style().set({
    fontSize: '18px',
    fontWeight: 'bold',
});
c.dataVarsET.text1.style().set({
    fontSize: '14px',
    margin: '4px 4px 4px 30px',
    color: 'purple',
});
c.dataVarsET.text2.style().set({
    fontSize: '14px',
    margin: '4px 4px 4px 30px',
    color: 'blue',
});
c.dataVarsET.text3.style().set({
    fontSize: '14px',
    margin: '4px 4px 4px 30px',
    color: 'orange',
});


// Data Sources 
c.dataSources.titleLabel.style().set({
    fontSize: '18px',
    fontWeight: 'bold',
});
c.dataSources.text1.style().set({
    fontSize: '14px',
    margin: '4px 4px 4px 30px',
});


// Set disclaimer style
c.disclaimer.titleLabel.style().set({
    fontSize: '18px',
    fontWeight: 'bold'
});
c.disclaimer.text.style().set({
    fontSize: '13px'
});

/*******************************************************************************
 * Behaviors *
 ******************************************************************************/

function updateModelNameLabel() {
    update_charts();
}
c.modelName.modelNameSelector.onChange(updateModelNameLabel);

function updateLandCoverLabel() {
    update_charts();
}
c.landCover.landCoverSelector.onChange(updateLandCoverLabel);

function updateAggChoice(agg_name) {
    var data = m.f_dict[c.aggType.aggTypeSelector.getValue()];
    var agg = ee.FeatureCollection(data.fc).filter(ee.Filter.eq('AGG_ID', agg_name)).first();
  
    // Center map to
    c.map_info.panel.style().set({
        shown: false
    });
    
    var lat = agg.geometry().centroid().coordinates().get(1).getInfo();
    var lon = agg.geometry().centroid().coordinates().get(0).getInfo();
    
    c.lat.setValue(lat);
    c.lon.setValue(lon);

    highlight_fc();
    update_charts();
}
c.aggName.Selector.onChange(updateAggChoice);

function updateLabelStart(yearStart) {
    update_charts();
}

function updateLabelEnd(endYear) {
    update_charts();
}
c.years.StartSelector.onChange(updateLabelStart);
c.years.EndSelector.onChange(updateLabelEnd);

function makeColorBarParams(palette) {
  return {
      bbox: [0, 0, 1, 0.1],
      dimensions: '150x20',
      format: 'png',
      min: 0,
      max: 1,
      palette: palette,
  };
}

function removeLayer(name) {
    var layers = Map.layers();
    // list of layers names
    var names = [];
    layers.forEach(function(lay) {
        var lay_name = lay.getName();
        names.push(lay_name);
    });
    // get index
    var index = names.indexOf(name);
    if (index > -1) {
        // if name in names
        var layer = layers.get(index);
        Map.remove(layer);
    }
}

function highlight_fc() {
    removeLayer('Feature of Interest');
    var point = ee.Geometry.Point([ee.Number.parse(c.lon.getValue()), ee.Number.parse(c.lat.getValue())]);
    var data = m.f_dict[c.aggType.aggTypeSelector.getValue()];
    var agg = ee.FeatureCollection(data.fc).filterBounds(point);
    c.mapPanel.layers().set(3, ui.Map.Layer(agg.style(s.HIGHLIGHT_STYLE), {}, 'Feature of Interest'));

    // zoom to layer
    c.mapPanel.centerObject(ee.Feature(agg.first()));
}

// Chart section


// ET COMPARISON
function chart_0() {
  var point = ee.Geometry.Point([
    ee.Number.parse(c.lon.getValue()),
    ee.Number.parse(c.lat.getValue())
  ]);

  var data = m.f_dict[c.aggType.aggTypeSelector.getValue()];
  var agg = ee.FeatureCollection(data.fc).filterBounds(point);
  var agg_name = agg.first().get('AGG_ID');

  var landcover = c.landCover.landCoverSelector.getValue();
  var startDate = c.years.StartSelector.getValue() + '-01-01';
  var endDate = (c.years.EndSelector.getValue() + 1) + '-01-01';

  // This is your desired legend/series order.
  var modelOrder = [
    'ENSEMBLE',
    'EEMETRIC',
    'DISALEXI',
    'GEESEBAL',
    'PTJPL',
    'SIMS',
    'SSEBOP'
  ];

  /*
   * Retrieve only the values required for the chart.
   *
   * Filtering ET_MEAN nulls here is important:
   * it prevents models with no plottable data from becoming null series.
   */
  var chartRows = m.all_data_tab
    .filter(ee.Filter.eq('AGG_ID', agg_name))
    .filter(ee.Filter.eq('LANDCOVER', landcover))
    .filter(ee.Filter.gte('DATE', startDate))
    .filter(ee.Filter.lt('DATE', endDate))
    .filter(ee.Filter.inList('MODEL', modelOrder))
    .filter(ee.Filter.notNull(['DATE', 'MODEL', 'ET_MEAN']))
    .map(function(ftr) {
      var date = ee.Date.parse(
        'yyyy-MM-dd',
        ee.String(ftr.get('DATE'))
      );

      // A compact chart-only row:
      // [milliseconds since epoch, model name, ET value]
      return ee.Feature(null, {
        row: ee.List([
          date.millis(),
          ftr.get('MODEL'),
          ftr.get('ET_MEAN')
        ])
      });
    })
    .aggregate_array('row');

  chartRows.evaluate(function(rows) {
    if (!rows || rows.length === 0) {
      c.charts.chart_0.widgets().set(
        0,
        ui.Label('No ET data are available for this selection.')
      );
      return;
    }

    /*
     * Identify models that truly have at least one valid ET_MEAN value.
     * Keep modelOrder order—not collection order.
     */
    var modelsPresent = {};

    rows.forEach(function(row) {
      modelsPresent[row[1]] = true;
    });

    var activeModels = modelOrder.filter(function(modelName) {
      return modelsPresent[modelName] === true;
    });

    if (activeModels.length === 0) {
      c.charts.chart_0.widgets().set(
        0,
        ui.Label('No valid ET data are available for this selection.')
      );
      return;
    }

    /*
     * Each color is associated with the corresponding active model.
     *
     * Example:
     * activeModels = ['ENSEMBLE', 'DISALEXI', 'SIMS']
     *
     * activeColors = [
     *   color_dict['ENSEMBLE'],
     *   color_dict['DISALEXI'],
     *   color_dict['SIMS']
     * ]
     */
    var activeColors = activeModels.map(function(modelName) {
      return color_dict[modelName];
    });

    // Map model name -> its guaranteed column position.
    var modelIndex = {};
    activeModels.forEach(function(modelName, index) {
      modelIndex[modelName] = index;
    });

    /*
     * Build one wide chart row per date:
     *
     * [Date, ENSEMBLE, EEMETRIC, DISALEXI, ...]
     *
     * Models missing on an individual date retain null. That is okay.
     * Models missing for the entire selection were removed above.
     */
    var rowsByDate = {};

    rows.forEach(function(row) {
      var millis = row[0];
      var modelName = row[1];
      var etMean = row[2];

      if (!rowsByDate[millis]) {
        var wideRow = [];
      
        // First column is the chart domain/date.
        wideRow.push(new Date(Number(millis)));
      
        // Add one null placeholder for each model/series.
        for (var i = 0; i < activeModels.length; i++) {
          wideRow.push(null);
        }
      
        rowsByDate[millis] = wideRow;
      }

      rowsByDate[millis][modelIndex[modelName] + 1] = etMean;
    });

    // Sort the wide rows chronologically.
    var chartDataRows = Object.keys(rowsByDate)
      .map(function(millis) {
        return rowsByDate[millis];
      })
      .sort(function(a, b) {
        return a[0].getTime() - b[0].getTime();
      });

    // Create the data-table header in exactly the same order as activeModels.
    var columnHeader = [{
      label: 'Date',
      role: 'domain',
      type: 'date'
    }];

    activeModels.forEach(function(modelName) {
      columnHeader.push({
        label: modelName,
        role: 'data',
        type: 'number'
      });
    });

    var chart = ui.Chart([columnHeader].concat(chartDataRows))
      .setChartType('LineChart')
      .setOptions({
        title: 'OpenET Actual ET Rates',
        titleTextStyle: s.textStyle,

        hAxis: {
          title: 'Date',
          titleTextStyle: {italic: false, bold: true},
          format: m.date_format
        },

        vAxis: {
          title: 'ETa (mm)',
          titleTextStyle: {italic: false, bold: true},
          viewWindowMode: 'explicit',
          viewWindow: {min: 0}
        },

        // Exact mapping: activeColors[i] belongs to activeModels[i].
        colors: activeColors,

        lineWidth: 2,
        pointSize: 1,

        // Leave gaps where an otherwise-present model lacks a date.
        interpolateNulls: false,

        legend: {
          position: 'top'
        }
      });

    c.charts.chart_0.widgets().set(0, chart);
  });
}


// ET RATE
function chart_1() {
    var point = ee.Geometry.Point([ee.Number.parse(c.lon.getValue()), ee.Number.parse(c.lat.getValue())]);
    var data = m.f_dict[c.aggType.aggTypeSelector.getValue()];
    var agg = ee.FeatureCollection(data.fc).filterBounds(point);
    var agg_name = agg.first().get("AGG_ID");
    var landcover = c.landCover.landCoverSelector.getValue();
    var model = c.modelName.modelNameSelector.getValue();

    var data_ftr = m.all_data_tab
      .filter(ee.Filter.eq('AGG_ID', agg_name))
      .filter(ee.Filter.eq('MODEL', model))
      .filter(ee.Filter.eq('LANDCOVER', landcover))
      .filter(ee.Filter.gte('DATE', c.years.StartSelector.getValue()+'-01-01'))
      .filter(ee.Filter.lt('DATE', (c.years.EndSelector.getValue()+1)+'-01-01'));

    var chart_data = data_ftr.map(function(ftr){
      var date_str = ee.Date.parse('yyyy-MM-dd', ftr.get('DATE'));
      var date_millis = date_str.millis().format();
      var date = ee.String('Date(').cat(date_millis).cat(')');
      
      var et_r = ftr.get('ET_MEAN');
      var et_r_25 = ftr.get('ET_PCT25');
      var et_r_75 = ftr.get('ET_PCT75');
      
      var row = ee.List([date, et_r, et_r_25, et_r_75]);
      
      return ee.Feature(null, {'row': row, 'Date': date_str});
    })
    .sort('Date');

    var dataTableServer = chart_data.aggregate_array('row');
  
    var columnHeader = ee.List([[
      {label: 'Date', role: 'domain', type:'date'},
      {label: 'ETa Mean', role:'data', type:'number'},
      {label: 'ETa 25th Percentile', role:'data', type:'number'},
      {label: 'ETa 75th Percentile', role:'data', type:'number'},
    ]]);
  
    dataTableServer = columnHeader.cat(dataTableServer);
  
    dataTableServer.evaluate(function(dataTableClient) {
      var chart = ui.Chart(dataTableClient).setOptions({
        title: 'OpenET ' + model + ' Actual ET Rates and Percentiles',
        titleTextStyle: s.textStyle,
        hAxis: {
          title: 'Date',
          titleTextStyle: {italic: false, bold: true},
          format: m.date_format
        },
        vAxis: {
          title: 'ETa (mm)',
          titleTextStyle: {italic: false, bold: true},
          viewWindowMode: 'explicit',
          viewWindow: {min: 0}
        },
        colors: [
          color_dict[model], 
          'lightgrey', 
          'lightgrey'
        ],
        legend: {
          position: 'none'
        },
        lineType: 'dashed',
      });
      c.charts.chart_1.widgets().set(0, chart);
    });
}


// PIXEL COUNT
function chart_2() {
    var point = ee.Geometry.Point([ee.Number.parse(c.lon.getValue()), ee.Number.parse(c.lat.getValue())]);
    var data = m.f_dict[c.aggType.aggTypeSelector.getValue()];
    var agg = ee.FeatureCollection(data.fc).filterBounds(point);
    var agg_name = agg.first().get("AGG_ID");
    var landcover = c.landCover.landCoverSelector.getValue();
    var model = c.modelName.modelNameSelector.getValue();

    var data_ftr = m.all_data_tab
      .filter(ee.Filter.eq('AGG_ID', agg_name))
      .filter(ee.Filter.eq('MODEL', model))
      .filter(ee.Filter.eq('LANDCOVER', landcover))
      .filter(ee.Filter.gte('DATE', c.years.StartSelector.getValue()+'-01-01'))
      .filter(ee.Filter.lt('DATE', (c.years.EndSelector.getValue()+1)+'-01-01'));

    var chart_data = data_ftr.map(function(ftr){
      var date_str = ee.Date.parse('yyyy-MM-dd', ftr.get('DATE'));
      var date_millis = date_str.millis().format();
      var date = ee.String('Date(').cat(date_millis).cat(')');
      
      var pxc = ftr.get('PERCENT_OF_MAX_PIXELS');
      
      var row = ee.List([date, pxc]);
      
      return ee.Feature(null, {'row': row, 'Date': date_str});
    })
    .sort('Date');

    var dataTableServer = chart_data.aggregate_array('row');
  
    var columnHeader = ee.List([[
      {label: 'Date', role: 'domain', type:'date'},
      {label: '% of Max Pixels', role:'data', type:'number'},
    ]]);
  
    dataTableServer = columnHeader.cat(dataTableServer);
  
    dataTableServer.evaluate(function(dataTableClient) {
      var chart = ui.Chart(dataTableClient).setOptions({
        title: 'OpenET ' + model + ' Percent of Max Available Pixels',
        titleTextStyle: s.textStyle,
        hAxis: {
          title: 'Date',
          titleTextStyle: {italic: false, bold: true},
          format: m.date_format
        },
        vAxis: {
          title: '% of Max Pixels',
          titleTextStyle: {italic: false, bold: true},
          // viewWindowMode: 'explicit',
          // viewWindow: {min: 0}
        },
        colors: ['blue'],
        legend: {
          position: 'none'
        },
      });
      c.charts.chart_2.widgets().set(0, chart);
    });
}



// MISSING ET MONTH
function chart_3() {
    var point = ee.Geometry.Point([ee.Number.parse(c.lon.getValue()), ee.Number.parse(c.lat.getValue())]);
    var data = m.f_dict[c.aggType.aggTypeSelector.getValue()];
    var agg = ee.FeatureCollection(data.fc).filterBounds(point);
    var agg_name = agg.first().get("AGG_ID");
    var landcover = c.landCover.landCoverSelector.getValue();
    var model = c.modelName.modelNameSelector.getValue();

    var data_ftr = m.all_data_tab
      .filter(ee.Filter.eq('AGG_ID', agg_name))
      .filter(ee.Filter.eq('MODEL', model))
      .filter(ee.Filter.eq('LANDCOVER', landcover))
      .filter(ee.Filter.gte('DATE', c.years.StartSelector.getValue()+'-01-01'))
      .filter(ee.Filter.lt('DATE', (c.years.EndSelector.getValue()+1)+'-01-01'));

    var chart_data = data_ftr.map(function(ftr){
      var date_str = ee.Date.parse('yyyy-MM-dd', ftr.get('DATE'));
      var date_millis = date_str.millis().format();
      var date = ee.String('Date(').cat(date_millis).cat(')');
      
      var flg = ftr.get('MISSING_ET_MONTH');
      
      var row = ee.List([date, flg]);
      
      return ee.Feature(null, {'row': row, 'Date': date_str});
    })
    .sort('Date');

    var dataTableServer = chart_data.aggregate_array('row');
  
    var columnHeader = ee.List([[
      {label: 'Date', role: 'domain', type:'date'},
      {label: 'Missing ET Months', role:'data', type:'number'},
    ]]);
  
    dataTableServer = columnHeader.cat(dataTableServer);
  
    dataTableServer.evaluate(function(dataTableClient) {
      var chart = ui.Chart(dataTableClient).setOptions({
        title: 'OpenET ' + model + ' Missing ET Months',
        titleTextStyle: s.textStyle,
        hAxis: {
          title: 'Date',
          titleTextStyle: {italic: false, bold: true},
          format: m.date_format
        },
        // vAxis: {
        //   title: 'Missing ET Flag',
        //   titleTextStyle: {italic: false, bold: true},
        //   // viewWindowMode: 'explicit',
        //   // viewWindow: {min: 0}
        // },
        vAxis: {
          title: 'Missing ET Month',
          titleTextStyle: {italic: false, bold: true},

          // Keep the plot constrained to Boolean-like values.
          viewWindow: {
            min: -0.1,
            max: 1.1
          },

          // Display Boolean labels while plotting numeric values.
          ticks: [
            {v: 0, f: 'False'},
            {v: 1, f: 'True'}
          ]
        },
        colors: ['chocolate'],
        legend: {
          position: 'none'
        },
      });
      c.charts.chart_3.widgets().set(0, chart);
    });
}


// Functions to check if charts can be rendered (checks if End Year >= Start Year)
function run_charts() {
    chart_0();
    chart_1();
    chart_2();
    chart_3();
}

function start_end_year() {
    c.charts.chart_1.widgets().set(0, ui.Label('Warning: Start Year is greater than End Year', {
        fontSize: '14px',
        fontWeight: 'bold',
        color: 'red'
    }));
}

// Check that charts can be rendered
function update_charts() {

    // Check if Start Year <= End year
    var total_years = ee.Number.parse(c.years.EndSelector.getValue())
                      .subtract(ee.Number.parse(c.years.StartSelector.getValue()));
    var greater_than = total_years.gte(ee.Number(0));

    if (!!greater_than.getInfo()) {
        // This is good, coninue to plot graphs
    } else {
        start_end_year();
        return null;
    }

    run_charts();
}

// Chart Check Box Function
function show_charts_0(checked) {
    c.charts.chart_0.style().set({
        shown: checked
    });
}
c.checkbox.chart_0.onChange(show_charts_0);

function show_charts_1(checked) {
    c.charts.chart_1.style().set({
        shown: checked
    });
}
c.checkbox.chart_1.onChange(show_charts_1);

function show_charts_2(checked) {
    c.charts.chart_2.style().set({
        shown: checked
    });
}
c.checkbox.chart_2.onChange(show_charts_2);


function show_charts_3(checked) {
    c.charts.chart_3.style().set({
        shown: checked
    });
}
c.checkbox.chart_3.onChange(show_charts_3);


// Download data Function
function download_data() {
    var agg_type = c.table_aggType.aggTypeSelector.getValue();
    var landcover = c.table_landCover.landCoverSelector.getValue();
    var model = c.table_modelName.modelNameSelector.getValue();
    var agg_name = c.aggName.Selector.getValue();
    var start_year = c.table_years.StartSelector.getValue();
    var end_year = c.table_years.EndSelector.getValue();
    
    var download_fc_et = m.all_data_tab
      .filter(ee.Filter.eq('AGG_ID', agg_name))
      .filter(ee.Filter.eq('LANDCOVER', landcover))
      .filter(ee.Filter.eq('MODEL', model))
      .filter(ee.Filter.gte('DATE', (start_year)+'-01-01'))
      .filter(ee.Filter.lt('DATE', (end_year+1)+'-01-01'))
      .sort('DATE');

    var url_et = ee.FeatureCollection(download_fc_et).getDownloadURL({
        format: 'csv',
        selectors: ['DATE', "MODEL", 'ET_MEAN', 'ET_MEDIAN', 'ET_PCT25', 'ET_PCT75', 'ET_STDDEV', 'PIXEL_COUNT', 'PERCENT_OF_MAX_PIXELS', 'MISSING_ET_MONTH'],
        filename: 'CADWR_ET_' + agg_type + '_' + landcover  + '_' + start_year + '_' + end_year + '_' + agg_name
    });


    c.d_link.d_label.setValue('Download Monthly ' + landcover + ' ' + model + ' ET data for ' + agg_name + ' ' + agg_type +' from ' + start_year + '-' + end_year);
    c.d_link.panel.clear();
    c.d_link.panel.widgets().set(0, c.d_link.d_label.setUrl(url_et));
}
c.download.button.onClick(download_data);

// Set button to reset zoom
function reset_zoom() {
    c.mapPanel.centerObject(ee.Geometry.Point([-120, 37.5]), 6.4);
}
// c.thumbnail.onClick(reset_zoom);

/*******************************************************************************
 * Initialize *
 *
 * A section to initialize the app state on load.
 *
 * Guidelines:
 * 1. At the top, define any helper functions.
 * 2. As much as possible, use URL params to initialize the state of the app.
 ******************************************************************************/
c.aggType.aggTypeSelector.setValue("HYDRO_REGION", false);
c.landCover.landCoverSelector.setValue("AG_LANDS", false);
c.modelName.modelNameSelector.setValue("ENSEMBLE", false);

c.years.StartSelector.setValue(ee.String(ee.Number.parse(ee.List(m.yearList).get(-1)).subtract(5)).getInfo(), false);
c.years.EndSelector.setValue(ee.List(m.yearList).get(-1).getInfo(), false);

c.table_years.StartSelector.setValue(ee.String(ee.Number.parse(ee.List(m.yearList).get(-1)).subtract(22)).getInfo(), false);
c.table_years.EndSelector.setValue(ee.List(m.yearList).get(-1).getInfo(), false);
c.table_modelName.modelNameSelector.setValue('ENSEMBLE', false);
c.table_landCover.landCoverSelector.setValue('AG_LANDS', false);
c.aggType.aggTypeSelector.setValue('HYDRO_REGION', false);
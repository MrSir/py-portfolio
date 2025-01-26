

breakdown_by_moniker_labels = []
breakdown_by_moniker = []
breakdown_by_moniker_data.forEach(function(point){
    breakdown_by_moniker_labels.push(point.moniker)
    breakdown_by_moniker.push((point.percent * 100).toFixed(2))
});

show_pie_legend = false
if (is_xlarge) {
  show_pie_legend = {position: "right"}
}

const breakdownByMonikerCanvas = document.getElementById('breakdownByMoniker');
new Chart(
  breakdownByMonikerCanvas,
  {
    type: 'pie',
    data: {
      labels: breakdown_by_moniker_labels,
      datasets: [
        {
          label: '%',
          data: breakdown_by_moniker,
          fill: false
        },
      ]
    },
    options: {
      responsive: true,
      plugins: {
        legend: show_pie_legend,
        title: {
          display: true,
          text: 'Percent of Portfolio by Moniker'
        }
      }
    },
  }
);

breakdown_by_stock_type_labels = []
breakdown_by_stock_type = []
breakdown_by_stock_type_data.forEach(function(point){
    breakdown_by_stock_type_labels.push(point.stock_type)
    breakdown_by_stock_type.push((point.percent * 100).toFixed(2))
});

const breakdownByStockTypeCanvas = document.getElementById('breakdownByStockType');
new Chart(
  breakdownByStockTypeCanvas,
  {
    type: 'pie',
    data: {
      labels: breakdown_by_stock_type_labels,
      datasets: [
        {
          label: '%',
          data: breakdown_by_stock_type,
          fill: false
        },
      ]
    },
    options: {
      responsive: true,
      plugins: {
        legend: show_pie_legend,
        title: {
          display: true,
          text: 'Percent of Portfolio by Stock Type'
        }
      }
    },
  }
);

breakdown_by_sector_labels = []
breakdown_by_sector = []
breakdown_by_sector_data.forEach(function(point){
    breakdown_by_sector_labels.push(point.sector)
    breakdown_by_sector.push((point.percent * 100).toFixed(2))
});

const breakdownBySectorCanvas = document.getElementById('breakdownBySector');
new Chart(
  breakdownBySectorCanvas,
  {
    type: 'pie',
    data: {
      labels: breakdown_by_sector_labels,
      datasets: [
        {
          label: '%',
          data: breakdown_by_sector,
          fill: false
        },
      ]
    },
    options: {
      responsive: true,
      plugins: {
        legend: show_pie_legend,
        title: {
          display: true,
          text: 'Percent of Portfolio by Sector'
        }
      }
    },
  }
);

growth_breakdown_equity_labels = []
growth_breakdown_equity_data_sets = []
growth_breakdown_equity_data_by_moniker = {}
growth_breakdown_by_stock_type_data.EQUITY.forEach(function(point){
  growth_breakdown_equity_labels.push(point.month)

  Object.keys(point).forEach(function(key){
    if (key != "month") {
      if (!(key in growth_breakdown_equity_data_by_moniker)) {
        growth_breakdown_equity_data_by_moniker[key] = []
      }
      growth_breakdown_equity_data_by_moniker[key].push((point[key] * 100).toFixed(2))
    }
  })
});
Object.keys(growth_breakdown_equity_data_by_moniker).forEach(function(key){
  growth_breakdown_equity_data_sets.push(
    {
      label: key,
      data: growth_breakdown_equity_data_by_moniker[key],
      fill: false,
    }
  )
});

show_legend = false
if (is_large || is_xlarge) {
  show_legend = {position: "right"}
}
const growthBreakdownEquityCanvas = document.getElementById('growthBreakdownEquity');
new Chart(
  growthBreakdownEquityCanvas,
  {
    type: 'line',
    data: {
      labels: growth_breakdown_equity_labels,
      datasets: growth_breakdown_equity_data_sets,
    },
    options: {
      responsive: true,
      plugins: {
        legend: show_legend,
        title: {
          display: true,
          text: 'Portfolio Growth(%) for Equity Stock'
        }
      }
    },
  }
);

growth_breakdown_etf_labels = []
growth_breakdown_etf_data_sets = []
growth_breakdown_etf_data_by_moniker = {}
growth_breakdown_by_stock_type_data.ETF.forEach(function(point){
  growth_breakdown_etf_labels.push(point.month)

  Object.keys(point).forEach(function(key){
    if (key != "month") {
      if (!(key in growth_breakdown_etf_data_by_moniker)) {
        growth_breakdown_etf_data_by_moniker[key] = []
      }
      growth_breakdown_etf_data_by_moniker[key].push((point[key] * 100).toFixed(2))
    }
  })
});
Object.keys(growth_breakdown_etf_data_by_moniker).forEach(function(key){
  growth_breakdown_etf_data_sets.push(
    {
      label: key,
      data: growth_breakdown_etf_data_by_moniker[key],
      fill: false,
    }
  )
});

const growthBreakdownETFCanvas = document.getElementById('growthBreakdownETF');
new Chart(
  growthBreakdownETFCanvas,
  {
    type: 'line',
    data: {
      labels: growth_breakdown_etf_labels,
      datasets: growth_breakdown_etf_data_sets,
    },
    options: {
      responsive: true,
      plugins: {
        legend: show_legend,
        title: {
          display: true,
          text: 'Portfolio Growth(%) for ETF Stock'
        }
      }
    },
  }
);

growth_breakdown_mom_equity_labels = []
growth_breakdown_mom_equity_data_sets = []
growth_breakdown_mom_equity_data_by_moniker = {}
growth_breakdown_mom_by_stock_type_data.EQUITY.forEach(function(point){
  growth_breakdown_mom_equity_labels.push(point.month)

  Object.keys(point).forEach(function(key){
    if (key != "month") {
      if (!(key in growth_breakdown_mom_equity_data_by_moniker)) {
        growth_breakdown_mom_equity_data_by_moniker[key] = []
      }
      growth_breakdown_mom_equity_data_by_moniker[key].push((point[key] * 100).toFixed(2))
    }
  })
});
Object.keys(growth_breakdown_mom_equity_data_by_moniker).forEach(function(key){
  growth_breakdown_mom_equity_data_sets.push(
    {
      label: key,
      data: growth_breakdown_mom_equity_data_by_moniker[key],
    }
  )
});

const growthBreakdownMonthOverMonthEquityCanvas = document.getElementById('growthBreakdownMonthOverMonthEquity');
new Chart(
  growthBreakdownMonthOverMonthEquityCanvas,
  {
    type: 'bar',
    data: {
      labels: growth_breakdown_mom_equity_labels,
      datasets: growth_breakdown_mom_equity_data_sets,
    },
    options: {
      responsive: true,
      plugins: {
        legend: show_legend,
        title: {
          display: true,
          text: 'Market Growth(%) for Equity Stock'
        }
      }
    },
  }
);

growth_breakdown_mom_etf_labels = []
growth_breakdown_mom_etf_data_sets = []
growth_breakdown_mom_etf_data_by_moniker = {}
growth_breakdown_mom_by_stock_type_data.ETF.forEach(function(point){
  growth_breakdown_mom_etf_labels.push(point.month)

  Object.keys(point).forEach(function(key){
    if (key != "month") {
      if (!(key in growth_breakdown_mom_etf_data_by_moniker)) {
        growth_breakdown_mom_etf_data_by_moniker[key] = []
      }
      growth_breakdown_mom_etf_data_by_moniker[key].push((point[key] * 100).toFixed(2))
    }
  })
});
Object.keys(growth_breakdown_mom_etf_data_by_moniker).forEach(function(key){
  growth_breakdown_mom_etf_data_sets.push(
    {
      label: key,
      data: growth_breakdown_mom_etf_data_by_moniker[key],
    }
  )
});

const growthBreakdownMonthOverMonthETFCanvas = document.getElementById('growthBreakdownMonthOverMonthETF');
new Chart(
  growthBreakdownMonthOverMonthETFCanvas,
  {
    type: 'bar',
    data: {
      labels: growth_breakdown_mom_etf_labels,
      datasets: growth_breakdown_mom_etf_data_sets,
    },
    options: {
      responsive: true,
      plugins: {
        legend: show_legend,
        title: {
          display: true,
          text: 'Market Growth(%) for ETF Stock'
        }
      }
    },
  }
);


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

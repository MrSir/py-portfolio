growth_labels = []
growth_invested = []
growth_value = []
growth_profit = []
growth_profit_ratio = []
growth_profit_ratio_difference = []
portfolio_ratio_invested = []
portfolio_ratio_earned = []
annual_growth_values = {}

growth_data.forEach(function(point){
    growth_labels.push(point.month)
    growth_invested.push(point.invested.toFixed(2))
    growth_value.push(point.value.toFixed(2))
    growth_profit.push(point.profit.toFixed(2))
    growth_profit_ratio.push((point.profit_ratio * 100).toFixed(2))
    growth_profit_ratio_difference.push((point.profit_ratio_difference * 100).toFixed(2))

    invested_ratio = (point.invested / point.value) * 100
    if (invested_ratio > 100) {
      invested_ratio = 100.00
    }

    earned_ratio = (point.profit / point.value) * 100
    if (earned_ratio > 100) {
      earned_ratio = 100.00
    }

    portfolio_ratio_invested.push(invested_ratio.toFixed(2))
    portfolio_ratio_earned.push(earned_ratio.toFixed(2))

    year_label = point.month.split('-')[1]
    if (!(year_label in annual_growth_values)) {
      annual_growth_values[year_label] = 0
    }
    annual_growth_values[year_label] += point.profit_ratio_difference
});

Object.keys(annual_growth_values).forEach(function (key) {
  annual_growth_values[key] = (annual_growth_values[key] * 100).toFixed(2)
});

const investedVSmarketCanvas = document.getElementById('investedVSmarket');
new Chart(
  investedVSmarketCanvas,
  {
    type: 'line',
    data: {
      labels: growth_labels,
      datasets: [
        {
          label: 'Invested',
          data: growth_invested,
          fill: false,
        },
        {
          label: 'Market',
          data: growth_value,
          fill: false
        }
      ]
    },
    options: {
      responsive: true,
      plugins: {
        title: {
          display: true,
          text: "Invested vs. Market Value"
        },
        legend: {
          position: 'right',
        },
      },
    },
  }
);

const profitGrowthCanvas = document.getElementById('profitGrowth');
new Chart(
  profitGrowthCanvas,
  {
    type: 'line',
    data: {
      labels: growth_labels,
      datasets: [
        {
          label: '$',
          data: growth_profit,
          fill: {
            target: "origin",
            below: 'rgba(255, 99, 132, 0.2)',
            above: 'rgba(75, 192, 192, 0.2)',
          }
        },
      ]
    },
    options: {
      responsive: true,
      plugins: {
        title: {
          display: true,
          text: "Profit Growth"
        },
        legend: false,
      },
    },
  }
);

const portfolioRatioCanvas = document.getElementById('portfolioRatio');
new Chart(
  portfolioRatioCanvas,
  {
    type: 'bar',
    data: {
      labels: growth_labels,
      datasets: [
        {
          label: 'Invested',
          data: portfolio_ratio_invested,
          fill: false,
        },
        {
          label: 'Earned',
          data: portfolio_ratio_earned,
          fill: false
        }
      ]
    },
    options: {
      responsive: true,
      plugins: {
        title: {
          display: true,
          text: "Portfolio Ratio"
        },
        legend: false,
      },
      scales: {
        x: {
          stacked: true,
        },
        y: {
          stacked: true
        }
      }
    },
  }
);

const red_color = 'rgba(255, 99, 132, 0.5)'
const green_color = 'rgba(75, 192, 192, 0.5)'

monthly_background_colors = []
growth_profit_ratio_difference.forEach(function(value){
  if (value < 0) {
    monthly_background_colors.push(red_color)
  } else {
    monthly_background_colors.push(green_color)
  }
});

const monthlyGrowthCanvas = document.getElementById('monthlyGrowth');
new Chart(
  monthlyGrowthCanvas,
  {
    type: 'bar',
    data: {
      labels: growth_labels,
      datasets: [
        {
          label: '%',
          data: growth_profit_ratio_difference,
          backgroundColor: monthly_background_colors,
        },
      ]
    },
    options: {
      responsive: true,
      plugins: {
        title: {
          display: true,
          text: "Monthly Growth"
        },
        legend: false,
      },
    },
  }
);

annual_background_colors = []
Object.values(annual_growth_values).forEach(function(value){
  if (value < 0) {
    annual_background_colors.push(red_color)
  } else {
    annual_background_colors.push(green_color)
  }
});
const annualGrowthCanvas = document.getElementById('annualGrowth');
new Chart(
  annualGrowthCanvas,
  {
    type: 'bar',
    data: {
      labels: Object.keys(annual_growth_values).map((x) => "'" + x),
      datasets: [
        {
          label: '%',
          data: Object.values(annual_growth_values),
          backgroundColor: annual_background_colors,
        },
      ]
    },
    options: {
      responsive: true,
      plugins: {
        title: {
          display: true,
          text: "Annual Growth"
        },
        legend: false,
      },
    },
  }
);
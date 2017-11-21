const rawData = [
  { x: 129295, y: 41 },
  { x: 292880, y: 64 },
  { x: 461398, y: 138 },
  { x: 624733, y: 244 },
  { x: 769895, y: 336 },
  { x: 905990, y: 437 },
  { x: 1035071, y: 565 },
  { x: 1154021, y: 590 },
  { x: 1266132, y: 629 },
  { x: 1373669, y: 720 },
  { x: 1476592, y: 750 },
  { x: 1572287, y: 798 },
  { x: 1667304, y: 842 },
  { x: 1753794, y: 874 },
  { x: 1837981, y: 927 },
  { x: 1920187, y: 953 },
  { x: 1991632, y: 994 },
  { x: 2059763, y: 1007 },
];


/* exact equation is y 0.000536968438542827000000000000x - 58.968638905686100000000000000000 */

const linearGraphData = [
  { x: 120000, y: 10.30038966 },
  { x: 2100000, y: 1068.665082 },
];

export const data = {
  datasets: [
    {
      label: 'measurement data',
      showLine: false,
      fill: false,
      data: rawData,
    },
    {
      label: 'linear',
      showLine: true,
      fill: false,
      data: linearGraphData,
    },
  ],
};

export const option = {
  scales: {
    xAxes: [{
      type: 'linear',
      position: 'bottom',
    }],
  },
};

export default {
  data,
  option,
};

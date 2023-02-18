from r2d2.training.data_loading.trajectory_sampler import *
from r2d2.training.data_loading.dataset import TrajectoryDataset
from r2d2.trajectory_utils.misc import visualize_timestep
from torch.utils.data.datapipes.iter import Shuffler

all_folderpaths = collect_data_folderpaths()
traj_sampler = TrajectorySampler(all_folderpaths)
traj_dataset = TrajectoryDataset(traj_sampler)
shuffled_dataset = Shuffler(traj_dataset, buffer_size=100)
data_iterator = iter(shuffled_dataset)

while True:
	timestep = next(data_iterator)
	visualize_timestep(timestep, pause_time=500)
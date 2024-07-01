use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use numpy::ToPyArray;
use numpy::PyArray1;
use numpy::PyArray2;

use rust_public_transit_sim::DynamicSimulator;
use rust_public_transit_sim::PtSystem;
use rust_public_transit_sim::PtVehicleType;
use rust_public_transit_sim::JourneyLeg;
use rust_public_transit_sim::VehicleState;
use rust_public_transit_sim::StaticSimulator;
use rust_public_transit_sim::Point2d;

// The time-aware simulator
#[pyclass]
struct PtDynamicSim {
    sim: DynamicSimulator,
}

#[pymethods]
impl PtDynamicSim {
    #[new]
    fn new(sim_config_path: &str) -> PtDynamicSim {
        PtDynamicSim{ sim: DynamicSimulator::new(sim_config_path).unwrap() }
    }

    fn set_pt_system(&mut self, vehicles_xml: &str, schedule_xml: &str) {
        let new_pt_sys = PtSystem::from_xml(vehicles_xml, schedule_xml);
        self.sim.set_transit_system(new_pt_sys);
    }

    fn run(&self) -> Vec<HashMap<String, String>> {
        let (trips, vehicles) = self.sim.run();
        // define a shorthand for this, since we'll use it often.
        let ptsys = self.sim.get_transit_system();

        // TODO convert trips and vehicles to XML.
        let mut events = vec![];
        for trip in trips {
            for leg in trip.get_real_journey() { match leg {
                JourneyLeg::PtLeg(ptl) => {
                    // event for boarding
                    let mut board = HashMap::new();
                    board.insert(String::from("time"), ptl.board_time_s.to_string());
                    board.insert(String::from("type"), String::from("PersonEntersVehicle"));
                    board.insert(String::from("person"), trip.id.clone());

                    let vehicle_id = ptsys.get_vehicle_by_route_and_dep(&ptl.route_id,
                                                                        &ptl.departure_id).unwrap();
                    board.insert(String::from("vehicle"), String::from(vehicle_id));
                    events.push(board);

                    // event for disembarking
                    if let Some(dts) = ptl.disembark_time_s {
                        let mut disembark = HashMap::new();
                        disembark.insert(String::from("time"),
                                         String::from(dts.to_string()));
                        disembark.insert(String::from("type"), String::from("PersonLeavesVehicle"));
                        disembark.insert(String::from("person"), trip.id.clone());
                        disembark.insert(String::from("vehicle"), String::from(vehicle_id));
                        events.push(disembark);
                    }
                },
                JourneyLeg::WalkingLeg(wl) => {
                    // start walking leg
                    let mut walkdep = HashMap::new();
                    walkdep.insert(String::from("time"), wl.start_time_s.to_string());
                    walkdep.insert(String::from("type"), String::from("departure"));
                    walkdep.insert(String::from("person"), trip.id.clone());
                    walkdep.insert(String::from("legMode"), String::from("transit_walk"));
                    events.push(walkdep);

                    // end walking leg
                    let mut walkarr = HashMap::new();
                    walkarr.insert(String::from("time"), wl.end_time_s.to_string());
                    walkarr.insert(String::from("type"), String::from("arrival"));
                    walkarr.insert(String::from("person"), trip.id.clone());
                    walkarr.insert(String::from("legMode"), String::from("transit_walk"));
                    events.push(walkarr);
                },
            }}
        }

        // handle vehicles
        for vehicle in vehicles {
            let mut prev_route = None;
            for leg in vehicle.get_real_journey() {
                if prev_route != leg.route_id && leg.route_id != None {
                    // new route starting!
                    let mut route_start = HashMap::new();
                    route_start.insert(String::from("time"), leg.start_time_s.to_string());
                    route_start.insert(String::from("type"), String::from("TransitDriverStarts"));
                    route_start.insert(String::from("vehicleId"), vehicle.id.clone());
                    if let Some(route) = &leg.route_id {
                        route_start.insert(String::from("transitLineId"), route.line_id.clone());
                        route_start.insert(String::from("transitRouteId"), route.route_id.clone());
                    }
                    if let Some(dep_id) = &leg.departure_id {
                        route_start.insert(String::from("departureId"), dep_id.clone());
                        events.push(route_start);
                    }
                }

                match &leg.state {
                    VehicleState::Idle(_) => {
                        // vehicle leaves traffic
                        let mut leaves_traffic = HashMap::new();
                        leaves_traffic.insert(String::from("time"), leg.start_time_s.to_string());
                        leaves_traffic.insert(String::from("type"),
                                              String::from("vehicle leaves traffic"));
                        leaves_traffic.insert(String::from("vehicle"), vehicle.id.clone());
                        leaves_traffic.insert(String::from("networkMode"), String::from("car"));
                        events.push(leaves_traffic);
                    },
                    VehicleState::AtStopOnRoute(_, stop_id) => {
                        // arrives at facility
                        let mut stoparr = HashMap::new();
                        stoparr.insert(String::from("time"), leg.start_time_s.to_string());
                        stoparr.insert(String::from("type"),
                                       String::from("VehicleArrivesAtFacility"));
                        stoparr.insert(String::from("vehicle"), vehicle.id.clone());
                        stoparr.insert(String::from("facility"), stop_id.clone());
                        events.push(stoparr);

                        // departs facility
                        if let Some(ets) = leg.end_time_s {
                            let mut stopdep = HashMap::new();
                            stopdep.insert(String::from("time"), ets.to_string());
                            stopdep.insert(String::from("type"),
                                           String::from("VehicleDepartsAtFacility"));
                            stopdep.insert(String::from("vehicle"), vehicle.id.clone());
                            stopdep.insert(String::from("facility"), stop_id.clone());
                            events.push(stopdep);
                        }
                    },
                    _ => {},
                }

                if prev_route != leg.route_id {
                    prev_route = leg.route_id.clone();
                }
            }
        }

        // sort events by time
        events.sort_unstable_by(|aa, bb| {
            let ta: u32 = aa.get("time").unwrap().parse().unwrap();
            let tb: u32 = bb.get("time").unwrap().parse().unwrap();
            ta.partial_cmp(&tb).unwrap()
        });

        return events;
        // // translate events to XML
        // let mut out_xml: Vec<u8> = Vec::new();
        // let mut writer = EventWriter::new(out_xml);
        // String::from("");
    }
}

// static simulator
#[pyclass]
struct StaticPtSim {
    sim: StaticSimulator,
}

#[pymethods]
impl StaticPtSim {
    #[new]
    fn new(sim_config_path: &str) -> StaticPtSim {
        let sim = StaticSimulator::from_cfg(sim_config_path);
        return StaticPtSim { sim };
    }

    fn run<'py>(&mut self, routes: Vec<Vec<usize>>, route_freqs_Hz: Vec<f64>,
                route_vehicles: Vec<&PyAny>, fast_demand_assignment: bool, py: Python<'py>)
                -> (HashMap<String, Vec<&'py PyArray1<f64>>>, HashMap<String, f64>) {
        // convert route_vehicles to a vec of PtVehicleTypes
        let route_vehicles = route_vehicles.iter().map(|pyvt|
            PtVehicleType::new(
                pyvt.getattr("id").unwrap().extract().unwrap(), 
                pyvt.getattr("mode").unwrap().extract().unwrap(), 
                pyvt.getattr("description").unwrap().extract().unwrap(),
                pyvt.getattr("seats").unwrap().extract().unwrap(),
                pyvt.getattr("standing_room").unwrap().extract().unwrap(),
                pyvt.getattr("length_m").unwrap().extract().unwrap(),
                pyvt.getattr("avg_power_kW").unwrap().extract().unwrap(),
            )
        ).collect();
            
        let result = self.sim.run(&routes, &route_freqs_Hz, &route_vehicles, 
                                  fast_demand_assignment);
        let mut glbls_dict = HashMap::new();
        glbls_dict.insert(String::from("total kW used"), result.power_used_kW);
        glbls_dict.insert(String::from("satisfied demand"), result.satisfied_demand);
        glbls_dict.insert(String::from("saved time"), result.total_saved_time);

        let mut perstops_dict = HashMap::new();
        let per_stop_satdem = result.per_stop_satisfied_demand.into_iter().
            map(|arr| arr.to_pyarray(py)).collect();
        perstops_dict.insert(String::from("satisfied demand"), per_stop_satdem);
        let stop_n_boarders = result.stop_n_boarders.into_iter().
            map(|arr| arr.to_pyarray(py)).collect();
        perstops_dict.insert(String::from("boarders"), stop_n_boarders);
        let stop_n_disembarkers = result.stop_n_disembarkers.into_iter().
            map(|arr| arr.to_pyarray(py)).collect();
        perstops_dict.insert(String::from("disembarkers"), stop_n_disembarkers);
        let per_stop_power = result.per_stop_power_used_kW.into_iter().
            map(|arr| arr.to_pyarray(py)).collect();
        perstops_dict.insert(String::from("power cost"), per_stop_power);
        return (perstops_dict, glbls_dict);
    }

    fn get_route_run_time_s<'py>(&self, route: Vec<usize>, mode: &str) -> f64 {
        let leg_times = self.sim.get_route_leg_times(&route, mode);
        return leg_times.sum();
    }

    fn get_stop_node_positions<'py>(&self, py: Python<'py>) -> &'py PyArray2<f64> {
        return match point2ds_to_array(py, self.sim.get_stop_node_positions().iter()) {
            Ok(ptr) => ptr,
            Err(err) => panic!("Something went wrong turning stop node positions into a numpy \
                                array! {:?}", err),
        };
    }

    fn get_node_index_by_id<'py>(&self, node_id: &str, py: Python<'py>) -> PyResult<usize> {
        return match self.sim.get_node_idx_by_id(node_id) {
            Some(idx) => Ok(idx),
            None => Err(PyValueError::new_err("Provided node id does not exist")),
        };
    }

    fn get_node_id_by_index<'py>(&self, node_index: usize, py: Python<'py>) -> PyResult<String> {
        return match self.sim.get_node_id_by_idx(node_index) {
            Some(id) => Ok(String::from(id)),
            None => Err(PyValueError::new_err("Provided node index does not exist")),
        };
    }

    fn get_drive_times_matrix<'py>(&self, mode: &str, py: Python<'py>) -> &'py PyArray2<f64> {
        let matrix = self.sim.get_travel_times_matrix(mode);
        return matrix.to_pyarray(py);
    }

    fn get_drive_dists_matrix<'py>(&self, mode: &str, py: Python<'py>) -> &'py PyArray2<f64> {
        let matrix = self.sim.get_drive_dists_matrix(mode);
        return matrix.to_pyarray(py);
    }

    /// return the vector of positions and the demand matrix
    fn get_demand_graph(&self) -> (Vec<(f64, f64)>, Vec<(usize, usize, f64)>) {
        let demand_graph = self.sim.get_demand_graph();
        let node_poss = demand_graph.node_weights().map(|vv| (vv.x_coord, vv.y_coord)).collect();
        return (node_poss, self.sim.get_demand_edges());
    }

    fn get_basin_connections(&self) -> HashMap<usize, HashMap<usize, f64>> {
        let basin_conns = self.sim.get_basin_connections();
        let mut new_basin_conns = HashMap::new();
        for (dni, dconns) in basin_conns {
            let mut new_dconns = HashMap::new();
            for (sni, time) in dconns {
                new_dconns.insert(sni.index() as usize, *time);
            }
            new_basin_conns.insert(dni.index() as usize, new_dconns);
        }

        return new_basin_conns;
    }


    fn get_street_edges(&self) -> Vec<(usize, usize)> {
        // Return a vector of node pairs.
        return self.sim.get_street_edges();
    }
}

fn point2ds_to_array<'py, 'a, Itr>(py: Python<'py>, points: Itr) 
                              -> Result<&'py PyArray2<f64>, numpy::FromVecError> 
                              where Itr: Iterator<Item = &'a Point2d> {
    let vec: Vec<Vec<f64>> = points.map(|pp| vec![pp.x_coord, pp.y_coord]).collect();
    return PyArray2::from_vec2(py, &vec);
}

#[pymodule]
fn ptsim(py: Python, module: &PyModule) -> PyResult<()> {
    module.add_class::<PtDynamicSim>()?;
    module.add_class::<StaticPtSim>()?;

    Ok(())
}

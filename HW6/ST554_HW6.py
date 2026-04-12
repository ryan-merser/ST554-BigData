import matplotlib.pyplot as plt
import numpy as np
from numpy.random import default_rng
from sklearn import linear_model

# Creating class for SLR slope simulation
class SLR_slope_simulator:
    """
    Simulates the sampling distribution of the slope estimator
    in a SLR model:
        y = beta_0 + beta_1 * x + epsilon,  epsilon ~ N(0, sigma^2)
    """

# Initialize class with parameter arguments
    def __init__(self, beta_0, beta_1, x, sigma, seed):
        """
        Initialize the simulator
        Parameters
        ----------
        beta_0 : float  – true intercept
        beta_1 : float  – true slope
        x      : array  – fixed predictor values
        sigma  : float  – standard deviation of the error term
        seed   : int    – random seed for reproducibility
        """
        self.beta_0 = beta_0
        self.beta_1 = beta_1
        self.x      = np.array(x)
        self.sigma  = sigma
        self.n      = len(x)
        self.rng    = default_rng(seed)
        self.slopes = []

    # Generate_data method
    def generate_data(self):
        """
        Generate one synthetic dataset from the true SLR model.

        Returns
        -------
        x : ndarray  – the fixed predictor values
        y : ndarray  – simulated responses
        """
        y = self.beta_0 + self.beta_1 * self.x + self.rng.normal(loc=0, scale=self.sigma, size=self.n)
        return self.x, y

    # fit_slope method
    def fit_slope(self, x, y):
        """
        Fit an SLR model to (x, y) and return the estimated slope.

        Parameters
        ----------
        x : array-like
        y : array-like

        Returns
        -------
        float – OLS slope estimate (beta_1 hat)
        """
        reg = linear_model.LinearRegression()
        reg.fit(np.array(x).reshape(-1, 1), np.array(y))
        return reg.coef_[0]

    # run_simulations method
    def run_simulations(self, num_simulations):
        """
        Run the simulation loop and store all slope estimates.

        Parameters
        ----------
        num_simulations : int – number of repetitions

        Side-effect
        -----------
        Replaces self.slopes with an ndarray of length num_simulations.
        """
        slope_estimates = np.zeros(num_simulations)
        for i in range(num_simulations):
            x, y = self.generate_data()
            slope_estimates[i] = self.fit_slope(x, y)
        self.slopes = slope_estimates

    # plot_sampling_distribution method
    def plot_sampling_distribution(self):
        """
        Plot a histogram of the simulated slope estimates.
        Prints an error message if run_simulations() has not been called yet.
        """
        if len(self.slopes) == 0:
            print("Error: run_simulations() must be called first before plotting.")
            return

        plt.figure(figsize=(8, 5))
        plt.hist(self.slopes, bins=40, edgecolor="black", color="steelblue", alpha=0.8)
        plt.axvline(self.beta_1, color="red", linestyle="--", linewidth=1.5,
                    label=f"True β₁ = {self.beta_1}")
        plt.xlabel("Estimated Slope (β̂₁)", fontsize=13)
        plt.ylabel("Frequency", fontsize=13)
        plt.title("Simulated Sampling Distribution of the Slope Estimator", fontsize=14)
        plt.legend()
        plt.tight_layout()
        plt.show()

    # find_prob method
    def find_prob(self, value, sided):
        """
        Approximate a probability from the simulated slope distribution.

        Parameters
        ----------
        value  : float  – reference value
        sided  : str    – one of {"above", "below", "two-sided"}

        Returns
        -------
        float – estimated probability (or None if simulations not run)
        """
        if len(self.slopes) == 0:
            print("Error: run_simulations() must be called first before computing probabilities.")
            return None

        sided = sided.lower()

        if sided == "above":
            prob = np.mean(self.slopes > value)
            print(f"P(β̂₁ > {value}) ≈ {prob:.4f}")
        elif sided == "below":
            prob = np.mean(self.slopes < value)
            print(f"P(β̂₁ < {value}) ≈ {prob:.4f}")
        elif sided == "two-sided":
            prob = np.mean(np.abs(self.slopes) > value)
            print(f"P(|β̂₁| > {value}) ≈ {prob:.4f}")
        else:
            raise ValueError("sided must be 'above', 'below', or 'two-sided'.")

        return prob
    

# Demo

if __name__ == "__main__":

    # Create an instance
    x_vals = np.array(list(np.linspace(start=0, stop=10, num=11)) * 3)

    sim = SLR_slope_simulator(
        beta_0=12,
        beta_1=2,
        x=x_vals,
        sigma=1,
        seed=10
    )

    # Call plot_sampling_distribution BEFORE run_simulations — should print error
    print("--- Calling plot_sampling_distribution() before run_simulations() ---")
    sim.plot_sampling_distribution()

    # Run 10,000 simulations
    print("\n--- Running 10,000 simulations ---")
    sim.run_simulations(10_000)

    # Plot the sampling distribution
    sim.plot_sampling_distribution()

    # Two-sided probability of |β̂₁| > 2.1
    print("\n--- Two-sided probability for value = 2.1 ---")
    sim.find_prob(value=2.1, sided="two-sided")

    # Print the first 10 simulated slopes (full array is sim.slopes)
    print("\n--- First 10 simulated slope values (sim.slopes[:10]) ---")
    print(sim.slopes[:10])

    print(f"\nTotal slopes stored: {len(sim.slopes)}")
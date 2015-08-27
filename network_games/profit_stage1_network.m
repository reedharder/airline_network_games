function g=profit_stage1_network(f_i,carrier_coef, carrier_freq_inds,carrier_Markets, current_markets)
    %initialize frequency variable vector
    freqs_vector = zeros(numel(carrier_coef),1);
    %initialize current location in concatenated frequency variable vector
    vector_loc = 1;
    for market_ind=1:numel(carrier_Markets)
        %frequencies for current market
        current_mkt_freqs = current_markets{market_ind};
        %assign appropriate f_i, variable being optimized, to variable
        %vector
        current_mkt_freqs(carrier_freq_inds(market_ind)) = f_i(market_ind);
        f = current_mkt_freqs;
        %get number of players in market
        players = numel(current_mkt_freqs);
        if (players == 1)
           vars=[1,f(1),f(1)^2];      
        elseif (players == 2)
           vars=[1,f(1),f(2),f(1)^2,f(2)^2,f(1)*f(2)];  
        elseif (players == 3)
           vars=[1,f(1),f(2),f(3),f(1)^2,f(2)^2,f(3)^2,f(1)*f(2),f(1)*f(3),f(2)*f(3)];   
        elseif (players == 4)
           vars=[1,f(1),f(2),f(3),f(4),f(1)^2,f(2)^2,f(3)^2,f(4)^2,f(1)*f(2),f(1)*f(3),f(1)*f(4),f(2)*f(3),f(2)*f(4),f(3)*f(4)]; 
        end
        %put current market into carrier vector at appropriate location 
        freqs_vector(vector_loc:vector_loc + numel(vars) -1) = vars;
        %increment index of next market vector in carrier vector
        vector_loc = vector_loc + numel(vars);        
    end  
    %dot product of full coefficient vector and frequency vector to get
    %profit
    g=-carrier_coef*freqs_vector;
    
